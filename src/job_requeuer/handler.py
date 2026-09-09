"""HLS NextGen job requeuer Lambda.

Consumes FAILURE_RETRYABLE events from the SQS retry queue, increments the
attempt counter, writes a new AWAITING canonical record event and state pointer,
then resubmits the job to AWS Batch.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
from typing import TYPE_CHECKING

from common import (
    AwsBatchClient,
    GranuleProcessingEvent,
    ProcessingEventRecord,
    ProcessingState,
    S3RecordStore,
)

if TYPE_CHECKING:
    from aws_lambda_typing.context import Context
    from aws_lambda_typing.events import SQSEvent

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


def job_requeuer(
    *,
    job_queue: str,
    job_definition_name: str,
    output_bucket: str,
    processing_bucket: str,
    event: SQSEvent,
) -> list[str]:
    """Requeue granule processing events from the retry queue."""
    batch = AwsBatchClient(queue=job_queue, job_definition=job_definition_name)
    store = S3RecordStore(bucket=processing_bucket)

    job_ids: list[str] = []
    ts = dt.datetime.now(tz=dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    for record in event["Records"]:
        prev_event = GranuleProcessingEvent.from_json(record["body"])
        next_event = prev_event.new_attempt()

        logger.info(
            "Requeueing %s attempt %d", next_event.output_granule_id, next_event.attempt
        )

        awaiting_record = ProcessingEventRecord(
            state=ProcessingState.AWAITING.name,
            ts=ts,
        )
        for src_id in next_event.source_granule_ids:
            store.append_canonical_event(
                source_granule_id=src_id,
                output_granule_id=next_event.output_granule_id,
                workflow=next_event.workflow,
                acquisition_date=next_event.acquisition_date,
                attempt=next_event.attempt,
                event=awaiting_record,
                shadow=False,
            )
            store.write_state_pointer(
                workflow=next_event.workflow,
                acquisition_date=next_event.acquisition_date,
                source_granule_id=src_id,
                attempt=next_event.attempt,
                new_state=ProcessingState.AWAITING,
                old_state=None,
                output_granule_id=next_event.output_granule_id,
            )

        job_id = batch.submit_job(event=next_event, output_bucket=output_bucket)
        job_ids.append(job_id)

    return job_ids


def handler(event: SQSEvent, context: Context) -> dict[str, list[str]]:
    """Lambda entry point for the SQS retry queue."""
    job_queue = os.environ["BATCH_QUEUE_NAME"]
    job_definition_name = os.environ["BATCH_JOB_DEFINITION_NAME"]
    processing_bucket = os.environ["PROCESSING_BUCKET_NAME"]
    output_bucket = os.environ.get("DEBUG_BUCKET") or os.environ["OUTPUT_BUCKET_NAME"]

    job_requeuer(
        job_queue=job_queue,
        job_definition_name=job_definition_name,
        output_bucket=output_bucket,
        processing_bucket=processing_bucket,
        event=event,
    )
    return {"batchItemFailures": []}
