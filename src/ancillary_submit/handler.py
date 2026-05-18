"""Ancillary-submit Lambda — per-granule Batch submission.

Triggered by messages on the internal ancillary-submit SQS queue. Each message
represents one AWAITING granule (fields: source_granule_id, output_granule_id,
attempt, acquisition_date).

For each granule this Lambda:
  1. Verifies ancillary data is available on S3.
  2. Submits an AWS Batch job.
  3. Writes a conditional SUBMITTED state pointer (IfNoneMatch='*') — if the
     pointer already exists a parallel invocation already claimed this granule
     and we skip silently.
  4. Deletes the AWAITING state pointer and appends to the canonical record.
"""

from __future__ import annotations

import datetime as dt
import json
import os
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
def process_granule(
    *,
    source_granule_id: str,
    output_granule_id: str,
    attempt: int,
    acquisition_date: str,
    aux_bucket: str,
    processing_bucket: str,
    batch_queue: str,
    job_definition: str,
    output_bucket: str,
) -> bool:
    """Check aux data and submit a Batch job for one granule.

    Returns True if a job was submitted, False if skipped.
    """
    s3_client = boto3.client("s3")
    store = S3RecordStore(bucket=processing_bucket)
    batch = AwsBatchClient(queue=batch_queue, job_definition=job_definition)

    try:
        granule_id = GranuleId.from_str(output_granule_id)
    except (ValueError, KeyError):
        logger.warning("Cannot parse output_granule_id=%s; skipping", output_granule_id)
        return False

    if not _check_aux_data(granule_id, aux_bucket, s3_client):
        logger.info("Ancillary not yet available for %s; skipping", output_granule_id)
        return False

    granule_event = GranuleProcessingEvent(
        workflow=_WORKFLOW,
        acquisition_date=acquisition_date,
        source_granule_ids=[source_granule_id],
        output_granule_id=output_granule_id,
        attempt=attempt,
    )

    # submit_job uses a deterministic clientRequestToken so retries return the
    # same Batch job rather than creating a duplicate.
    job_id = batch.submit_job(event=granule_event, output_bucket=output_bucket)

    # Conditional write is the idempotency gate for the state-pointer side.
    # With an idempotent Batch submit above, a retry that loses the race here
    # just skips the pointer/record writes — the winning invocation handles them.
    written = store.write_state_pointer_conditional(
        workflow=_WORKFLOW,
        acquisition_date=acquisition_date,
        source_granule_id=source_granule_id,
        attempt=attempt,
        state=ProcessingState.SUBMITTED,
        output_granule_id=output_granule_id,
    )
    if not written:
        logger.info(
            "SUBMITTED pointer already exists for %s attempt=%d; skipping",
            source_granule_id,
            attempt,
        )
        return False

    ts = dt.datetime.now(tz=dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    store.delete_state_pointer(
        workflow=_WORKFLOW,
        acquisition_date=acquisition_date,
        source_granule_id=source_granule_id,
        attempt=attempt,
        state=ProcessingState.AWAITING,
    )
    store.append_canonical_event(
        source_granule_id=source_granule_id,
        output_granule_id=output_granule_id,
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
    logger.info("Submitted %s job_id=%s", output_granule_id, job_id)
    return True


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, int]:
    """Lambda entry point for per-granule ancillary-submit messages."""
    aux_bucket = os.environ["AUX_DATA_BUCKET_NAME"]
    processing_bucket = os.environ["PROCESSING_BUCKET_NAME"]
    batch_queue = os.environ["BATCH_QUEUE_NAME"]
    job_definition = os.environ["SENTINEL_JOB_DEFINITION_NAME"]
    output_bucket = os.environ["OUTPUT_BUCKET_NAME"]

    submitted = 0
    for record in event.get("Records", []):
        msg = json.loads(record["body"])
        if process_granule(
            source_granule_id=msg["source_granule_id"],
            output_granule_id=msg["output_granule_id"],
            attempt=msg["attempt"],
            acquisition_date=msg["acquisition_date"],
            aux_bucket=aux_bucket,
            processing_bucket=processing_bucket,
            batch_queue=batch_queue,
            job_definition=job_definition,
            output_bucket=output_bucket,
        ):
            submitted += 1

    return {"submitted": submitted}
