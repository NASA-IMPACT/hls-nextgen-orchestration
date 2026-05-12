"""Ancillary-trigger Lambda.

Triggered when new ancillary (LaSRC LADS) data lands on S3.  Scans
``state/AWAITING/sentinel/{acquisition_date}/`` for granules that are now
ready to process and submits an AWS Batch job for each one.

Idempotency is achieved via a conditional S3 PutObject (IfNoneMatch='*') on
the SUBMITTED state pointer: if a parallel invocation already wrote the
pointer the conditional write fails and we skip that granule silently.

Reserved concurrent executions = 1 in CDK prevents parallel scans on the
same dated prefix from racing, but the conditional write ensures correctness
even if that concurrency limit is ever relaxed.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
from typing import Any

import boto3
from aws_lambda_powertools import Logger, Tracer
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

_WORKFLOW = "sentinel"

# Matches: lasrc_aux/LADS/YYYY/VJ104ANC.AYYYYDDD  or VNP04ANC.AYYYYDDD
_AUX_KEY_RE = re.compile(r"lasrc_aux/LADS/(\d{4})/(?:VJ104ANC|VNP04ANC)\.A(\d{7})")


def _parse_aux_key(s3_key: str) -> str | None:
    """Extract YYYY-MM-DD acquisition date from an ancillary S3 key.

    Returns None if the key does not match the expected pattern.
    """
    m = _AUX_KEY_RE.search(s3_key)
    if not m:
        return None
    ydoy = m.group(2)  # YYYYDDD
    try:
        acq_dt = dt.datetime.strptime(ydoy, "%Y%j")
        return acq_dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def _key_pattern_exists(bucket: str, prefix: str, s3_client: Any) -> bool:
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return bool(resp.get("KeyCount", 0) > 0)


def _check_aux_data(granule_id: GranuleId, aux_bucket: str, s3_client: Any) -> bool:
    year = granule_id.begin_datetime.strftime("%Y")
    ydoy = granule_id.begin_datetime.strftime("%Y%j")
    return _key_pattern_exists(
        aux_bucket, f"lasrc_aux/LADS/{year}/VJ104ANC.A{ydoy}", s3_client
    ) or _key_pattern_exists(
        aux_bucket, f"lasrc_aux/LADS/{year}/VNP04ANC.A{ydoy}", s3_client
    )


@tracer.capture_method
def process_aux_event(
    *,
    s3_key: str,
    aux_bucket: str,
    processing_bucket: str,
    batch_queue: str,
    job_definition: str,
    output_bucket: str,
) -> int:
    """Scan AWAITING granules for *s3_key*'s date and submit ready ones.

    Returns the number of jobs submitted.
    """
    acquisition_date = _parse_aux_key(s3_key)
    if acquisition_date is None:
        logger.info("Key %s is not a recognised ancillary file; skipping", s3_key)
        return 0

    s3_client = boto3.client("s3")
    store = S3RecordStore(bucket=processing_bucket)
    batch = AwsBatchClient(queue=batch_queue, job_definition=job_definition)

    ts = dt.datetime.now(tz=dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    awaiting = store.list_awaiting(_WORKFLOW, acquisition_date)
    logger.info(
        "Found %d AWAITING granules for date=%s", len(awaiting), acquisition_date
    )

    submitted = 0
    for ptr in awaiting:
        src_id: str = ptr["source_granule_id"]
        output_id: str = ptr["output_granule_id"]
        attempt: int = ptr["attempt"]

        # Verify ancillary is still available (handles transient list glitches)
        try:
            granule_id = GranuleId.from_str(output_id)
        except (ValueError, KeyError):
            logger.warning("Cannot parse output_granule_id=%s; skipping", output_id)
            continue

        if not _check_aux_data(granule_id, aux_bucket, s3_client):
            logger.info("Ancillary not yet available for %s; skipping", output_id)
            continue

        granule_event = GranuleProcessingEvent(
            workflow=_WORKFLOW,
            acquisition_date=acquisition_date,
            source_granule_ids=[src_id],
            output_granule_id=output_id,
            attempt=attempt,
        )

        job_id = batch.submit_job(event=granule_event, output_bucket=output_bucket)

        # Conditional write: only one invocation may claim SUBMITTED for this granule
        written = store.write_state_pointer_conditional(
            workflow=_WORKFLOW,
            acquisition_date=acquisition_date,
            source_granule_id=src_id,
            attempt=attempt,
            state=ProcessingState.SUBMITTED,
            output_granule_id=output_id,
        )
        if not written:
            logger.info(
                "SUBMITTED pointer already exists for %s attempt=%d; skipping",
                src_id,
                attempt,
            )
            continue

        store.delete_state_pointer(
            workflow=_WORKFLOW,
            acquisition_date=acquisition_date,
            source_granule_id=src_id,
            attempt=attempt,
            state=ProcessingState.AWAITING,
        )
        store.append_canonical_event(
            source_granule_id=src_id,
            output_granule_id=output_id,
            workflow=_WORKFLOW,
            acquisition_date=acquisition_date,
            attempt=attempt,
            event=ProcessingEventRecord(
                state=ProcessingState.SUBMITTED.name,
                ts=ts,
                batch_job_id=job_id,
            ),
            batch_job_id=job_id,
            shadow=False,
        )
        logger.info("Submitted %s job_id=%s", output_id, job_id)
        submitted += 1

    return submitted


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, int]:
    """Lambda entry point for ancillary S3 PutObject events (SNS → SQS)."""
    processing_bucket = os.environ["PROCESSING_BUCKET_NAME"]
    aux_bucket = os.environ["AUX_DATA_BUCKET_NAME"]
    batch_queue = os.environ["BATCH_QUEUE_NAME"]
    job_definition = os.environ["SENTINEL_JOB_DEFINITION_NAME"]
    output_bucket = os.environ["OUTPUT_BUCKET_NAME"]

    total_submitted = 0
    for record in event.get("Records", []):
        body = json.loads(record["body"])
        # SNS-wrapped S3 event
        if "Message" in body:
            s3_event = json.loads(body["Message"])
            s3_records = s3_event.get("Records", [])
        else:
            s3_records = body.get("Records", [])

        for s3_record in s3_records:
            s3_key = s3_record["s3"]["object"]["key"]
            total_submitted += process_aux_event(
                s3_key=s3_key,
                aux_bucket=aux_bucket,
                processing_bucket=processing_bucket,
                batch_queue=batch_queue,
                job_definition=job_definition,
                output_bucket=output_bucket,
            )

    return {"submitted": total_submitted}
