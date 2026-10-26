"""Ancillary-submit Lambda -- per-granule Batch submission.

Triggered by messages on the internal ancillary-submit SQS queue. Each message
represents one output granule awaiting ancillary data (fields:
acquisition_date, source_granule_ids, output_granule_id, attempt).

For each granule this Lambda:
  1. Verifies ancillary data is available on S3.
  2. Claims its source granules with conditional SUBMITTED state pointers
     (IfNoneMatch='*') -- if one already exists a parallel invocation claimed
     the granule and we skip silently.
  3. Submits an AWS Batch job and deletes the AWAITING_ANCILLARY pointers.
"""

from __future__ import annotations

import json
import os
from typing import Any

import boto3
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext

from common import GranuleId
from common.ancillary import check_aux_data
from common.jobs import claim_and_submit, record_store, sentinel_job_group

logger = Logger()
tracer = Tracer()


@tracer.capture_method
def process_granule(
    *,
    acquisition_date: str,
    source_granule_ids: list[str],
    output_granule_id: str,
    attempt: int,
    aux_bucket: str,
    batch_queue: str,
    job_definition: str,
    output_bucket: str,
) -> bool:
    """Check aux data and submit a Batch job for one output granule.

    Returns True if a job was submitted, False if skipped.
    """
    try:
        granule_id = GranuleId.from_str(output_granule_id)
    except (ValueError, KeyError):
        logger.warning("Cannot parse output_granule_id=%s; skipping", output_granule_id)
        return False

    if not check_aux_data(granule_id, aux_bucket, boto3.client("s3")):
        logger.info("Ancillary not yet available for %s; skipping", output_granule_id)
        return False

    job_id = claim_and_submit(
        store=record_store(),
        batch_client=boto3.client("batch"),
        job_group=sentinel_job_group(
            acquisition_date=acquisition_date,
            source_granule_ids=source_granule_ids,
            output_granule_id=output_granule_id,
            attempt=attempt,
        ),
        job_queue=batch_queue,
        job_definition=job_definition,
        output_bucket=output_bucket,
    )
    if job_id is None:
        logger.info(
            "%s attempt=%d already claimed; skipping", output_granule_id, attempt
        )
        return False

    logger.info("Submitted %s job_id=%s", output_granule_id, job_id)
    return True


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, int]:
    """Lambda entry point for per-granule ancillary-submit messages."""
    aux_bucket = os.environ["AUX_DATA_BUCKET_NAME"]
    batch_queue = os.environ["BATCH_QUEUE_NAME"]
    job_definition = os.environ["SENTINEL_JOB_DEFINITION_NAME"]
    output_bucket = os.environ["OUTPUT_BUCKET_NAME"]

    submitted = 0
    for record in event.get("Records", []):
        msg = json.loads(record["body"])
        if process_granule(
            acquisition_date=msg["acquisition_date"],
            source_granule_ids=msg["source_granule_ids"],
            output_granule_id=msg["output_granule_id"],
            attempt=msg["attempt"],
            aux_bucket=aux_bucket,
            batch_queue=batch_queue,
            job_definition=job_definition,
            output_bucket=output_bucket,
        ):
            submitted += 1

    return {"submitted": submitted}
