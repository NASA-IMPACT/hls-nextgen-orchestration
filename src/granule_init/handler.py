"""Granule-init Lambda.

Triggered by S3 object-created events on the Sentinel-2 input bucket
(delivered via SNS -> SQS).  For each arriving SAFE granule:

  1. Detect twin granules (two SAFE IDs per MGRS tile) via S3 LIST.
  2. Derive output_granule_id and acquisition_date from the SAFE ID.
  3. Check ancillary data availability.
     - Available  -> submit Batch job, write SUBMITTED canonical record +
                     state pointer.
     - Unavailable -> write AWAITING canonical record + state pointer.
"""

from __future__ import annotations

import datetime as dt
import json
import os
from typing import Any

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext

from common import (
    AwsBatchClient,
    GranuleId,
    GranuleProcessingEvent,
    ProcessingEventRecord,
    ProcessingState,
    S3RecordStore,
)

logger = Logger()
tracer = Tracer()
metrics = Metrics(namespace="hls_granule_init")

_WORKFLOW = "sentinel"


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
def convert_safe_id_to_hls_id(safe_id: str) -> str:
    """Convert a Sentinel-2 SAFE ID to an HLS S30 granule ID."""
    parts = safe_id.split("_")
    date_str = parts[2][:15]
    year, month, day = date_str[0:4], date_str[4:6], date_str[6:8]
    hms = date_str[8:15]
    acq_dt = dt.datetime.strptime(f"{year}{month}{day}", "%Y%m%d")
    doy = f"{acq_dt.timetuple().tm_yday:03d}"
    tile = parts[5]
    return f"HLS.S30.{tile}.{year}{doy}{hms}.v2.0"


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
def key_pattern_exists(bucket: str, prefix: str, s3_client: Any) -> bool:
    """Return True if any S3 objects match *prefix* in *bucket*."""
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return bool(resp.get("KeyCount", 0) > 0)


@tracer.capture_method
def check_aux_data(granule_id: GranuleId, aux_bucket: str, s3_client: Any) -> bool:
    """Return True if ancillary (LaSRC LADS) data exists for *granule_id*'s date."""
    year = granule_id.begin_datetime.strftime("%Y")
    ydoy = granule_id.begin_datetime.strftime("%Y%j")
    vj_prefix = f"lasrc_aux/LADS/{year}/VJ104ANC.A{ydoy}"
    vnp_prefix = f"lasrc_aux/LADS/{year}/VNP04ANC.A{ydoy}"
    return key_pattern_exists(aux_bucket, vj_prefix, s3_client) or key_pattern_exists(
        aux_bucket, vnp_prefix, s3_client
    )


@tracer.capture_method
def process_record(sqs_body: str) -> None:
    """Process one SQS message (SNS-wrapped S3 event)."""
    processing_bucket = os.environ["PROCESSING_BUCKET_NAME"]
    sentinel_bucket = os.environ["SENTINEL_BUCKET_NAME"]
    aux_bucket = os.environ["AUX_DATA_BUCKET_NAME"]
    output_bucket = os.environ["OUTPUT_BUCKET_NAME"]
    batch_queue = os.environ["BATCH_QUEUE_NAME"]
    job_definition = os.environ["SENTINEL_JOB_DEFINITION_NAME"]
    max_active_jobs = int(os.environ["MAX_ACTIVE_JOBS"])

    s3_client = boto3.client("s3")
    store = S3RecordStore(bucket=processing_bucket)
    batch = AwsBatchClient(queue=batch_queue, job_definition=job_definition)

    ts = dt.datetime.now(tz=dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    s3_records = parse_s3_sqs_message(sqs_body)
    for s3_record in s3_records:
        s3_info = s3_record["s3"]
        object_key = s3_info["object"]["key"]

        safe_id = extract_safe_id_from_s3_key(object_key)
        output_granule_id = convert_safe_id_to_hls_id(safe_id)
        granule_id = GranuleId.from_str(output_granule_id)
        acquisition_date = granule_id.begin_datetime.strftime("%Y-%m-%d")

        # Twin detection: LIST the sentinel bucket for partner granule
        source_granule_ids = detect_twin_safe_ids(sentinel_bucket, safe_id, s3_client)

        granule_event = GranuleProcessingEvent(
            workflow=_WORKFLOW,
            acquisition_date=acquisition_date,
            source_granule_ids=source_granule_ids,
            output_granule_id=output_granule_id,
            attempt=0,
        )

        aux_available = check_aux_data(granule_id, aux_bucket, s3_client)

        if aux_available:
            if not batch.active_jobs_below_threshold(max_active_jobs):
                logger.info(
                    "Too many active jobs; deferring %s as AWAITING", output_granule_id
                )
                _write_awaiting(store, granule_event, source_granule_ids, ts)
                return

            job_id = batch.submit_job(event=granule_event, output_bucket=output_bucket)
            logger.info("Submitted %s job_id=%s", output_granule_id, job_id)
            submitted_event = ProcessingEventRecord(
                state=ProcessingState.SUBMITTED.name,
                ts=ts,
                batch_job_id=job_id,
            )
            for src_id in source_granule_ids:
                store.append_canonical_event(
                    source_granule_id=src_id,
                    output_granule_id=output_granule_id,
                    workflow=_WORKFLOW,
                    acquisition_date=acquisition_date,
                    attempt=0,
                    event=submitted_event,
                    batch_job_id=job_id,
                    shadow=False,
                )
                store.write_state_pointer(
                    workflow=_WORKFLOW,
                    acquisition_date=acquisition_date,
                    source_granule_id=src_id,
                    attempt=0,
                    new_state=ProcessingState.SUBMITTED,
                    old_state=None,
                    output_granule_id=output_granule_id,
                )
        else:
            logger.info("Ancillary unavailable; marking %s AWAITING", output_granule_id)
            _write_awaiting(store, granule_event, source_granule_ids, ts)


def _write_awaiting(
    store: S3RecordStore,
    event: GranuleProcessingEvent,
    source_granule_ids: list[str],
    ts: str,
) -> None:
    awaiting_record = ProcessingEventRecord(
        state=ProcessingState.AWAITING.name,
        ts=ts,
    )
    for src_id in source_granule_ids:
        store.append_canonical_event(
            source_granule_id=src_id,
            output_granule_id=event.output_granule_id,
            workflow=event.workflow,
            acquisition_date=event.acquisition_date,
            attempt=event.attempt,
            event=awaiting_record,
            shadow=False,
        )
        store.write_state_pointer(
            workflow=event.workflow,
            acquisition_date=event.acquisition_date,
            source_granule_id=src_id,
            attempt=event.attempt,
            new_state=ProcessingState.AWAITING,
            old_state=None,
            output_granule_id=event.output_granule_id,
        )


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
