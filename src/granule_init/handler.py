"""Granule-init Lambda.

Triggered by S3 object-created events on the Sentinel-2 input bucket
(delivered via SNS -> SQS).  For each arriving SAFE granule:

  1. Detect twin granules (two SAFE IDs per MGRS tile) via S3 LIST.
  2. Derive output_granule_id and acquisition_date from the SAFE ID.
  3. Check ancillary data availability.
     - Available   -> claim the granules and submit a Batch job.
     - Unavailable -> record the granules as AWAITING_ANCILLARY.
"""

from __future__ import annotations

import json
import os
from typing import Any

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext

from common import AwsBatchClient, GranuleId, convert_safe_id_to_hls_id
from common.ancillary import check_aux_data
from common.jobs import (
    claim_and_submit,
    record_store,
    sentinel_job_group,
    write_awaiting_ancillary,
)

logger = Logger()
tracer = Tracer()
metrics = Metrics(namespace="hls_granule_init")


@tracer.capture_method
def parse_s3_sqs_message(sqs_body: str) -> list[dict[str, Any]]:
    """Unwrap S3 event records from an SQS message body.

    Handles direct S3->SQS payloads, SNS-wrapped S3->SNS->SQS payloads, and
    the S3 test notification that AWS sends when event notifications are first
    configured (returns an empty list for test events).
    """
    body = json.loads(sqs_body)
    # Direct S3->SQS notification
    if "Records" in body:
        return body["Records"]  # type: ignore[no-any-return]
    # AWS s3:TestEvent sent on notification setup — nothing to process
    if body.get("Event") == "s3:TestEvent":
        logger.info("Received S3 test event; skipping")
        return []
    # SNS envelope: Message field holds the S3 event as a JSON string
    s3_event = json.loads(body["Message"])
    return s3_event.get("Records", [])  # type: ignore[no-any-return]


@tracer.capture_method
def extract_safe_id_from_s3_key(s3_key: str) -> str:
    """Extract SAFE ID (filename without extension) from an S3 object key."""
    filename = os.path.basename(s3_key)
    return os.path.splitext(filename)[0]


@tracer.capture_method
def detect_twin_safe_ids(bucket: str, safe_id: str, s3_client: Any) -> list[str]:
    """Return all SAFE IDs for the same satellite pass as *safe_id*.

    Two ESA Level-1C granules from the same pass share a common prefix
    (satellite, sensing datetime, tile) that differs only in the
    processing-datetime suffix.  We strip the processing timestamp and LIST
    to find any partner granule.

    Returns a list with 1 element (single) or 2 elements (twin).
    """
    # SAFE ID format: S2A_MSIL1C_YYYYMMDDTHHMMSS_Nxxxx_Rxxx_Txxxxx_YYYYMMDDTHHMMSS
    # The last component is the processing timestamp; strip it to get a shared prefix.
    parts = safe_id.split("_")
    if len(parts) < 7:
        return [safe_id]
    common_prefix_parts = parts[:6]
    common_prefix = "input/" + "_".join(common_prefix_parts) + "_"

    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=common_prefix)
    keys = [
        os.path.splitext(os.path.basename(obj["Key"]))[0]
        for obj in resp.get("Contents", [])
    ]
    # Deduplicate and sort for determinism
    unique_ids = sorted(set(keys)) if keys else [safe_id]
    return unique_ids if unique_ids else [safe_id]


@tracer.capture_method
def process_record(sqs_body: str) -> None:
    """Process one SQS message (SNS-wrapped S3 event)."""
    sentinel_bucket = os.environ["SENTINEL_BUCKET_NAME"]
    aux_bucket = os.environ["AUX_DATA_BUCKET_NAME"]
    output_bucket = os.environ["OUTPUT_BUCKET_NAME"]
    batch_queue = os.environ["BATCH_QUEUE_NAME"]
    job_definition = os.environ["SENTINEL_JOB_DEFINITION_NAME"]
    max_active_jobs = int(os.environ["MAX_ACTIVE_JOBS"])

    s3_client = boto3.client("s3")
    store = record_store()
    batch = AwsBatchClient(queue=batch_queue)

    for s3_record in parse_s3_sqs_message(sqs_body):
        object_key = s3_record["s3"]["object"]["key"]

        safe_id = extract_safe_id_from_s3_key(object_key)
        output_granule_id = convert_safe_id_to_hls_id(safe_id)
        granule_id = GranuleId.from_str(output_granule_id)

        job_group = sentinel_job_group(
            acquisition_date=granule_id.begin_datetime.strftime("%Y-%m-%d"),
            # Twin detection: LIST the sentinel bucket for partner granule
            source_granule_ids=detect_twin_safe_ids(
                sentinel_bucket, safe_id, s3_client
            ),
            output_granule_id=output_granule_id,
        )

        if not check_aux_data(granule_id, aux_bucket, s3_client):
            logger.info("Ancillary unavailable; deferring %s", output_granule_id)
            write_awaiting_ancillary(store, job_group)
            continue

        if not batch.active_jobs_below_threshold(max_active_jobs):
            logger.info("Too many active jobs; deferring %s", output_granule_id)
            write_awaiting_ancillary(store, job_group)
            continue

        job_id = claim_and_submit(
            store=store,
            batch_client=batch.client,
            job_group=job_group,
            job_queue=batch_queue,
            job_definition=job_definition,
            output_bucket=output_bucket,
        )
        if job_id is None:
            logger.info("%s already claimed; skipping", output_granule_id)
        else:
            logger.info("Submitted %s job_id=%s", output_granule_id, job_id)


@logger.inject_lambda_context
@tracer.capture_lambda_handler
@metrics.log_metrics
def handler(event: dict[str, Any], context: LambdaContext) -> None:
    """Lambda entry point for Sentinel-2 granule arrival events."""
    records = event.get("Records", [])
    if not records:
        logger.warning("No records in event")
        return
    if len(records) > 1:
        logger.warning("Multiple records received; processing only the first")
    process_record(records[0]["body"])
