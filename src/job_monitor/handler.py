"""HLS NextGen job monitor Lambda.

Consumes EventBridge Batch job state change events (SUCCEEDED / FAILED) and:

  - Determines outcome (SUCCESS, CLOUDY, LOW_SUN_ANGLE, FAILURE_RETRYABLE,
    FAILURE_NONRETRYABLE) from the exit code / status reason.
  - Writes a canonical record (append-only events array) and a state pointer
    for each source granule.
  - Writes an output index entry for all terminal states.
  - Routes FAILURE_RETRYABLE (on final Batch attempt) → SQS retry queue.
  - Routes FAILURE_NONRETRYABLE → SQS failure DLQ.

Supports two operating modes detected from Batch job env vars:

  Phase 1 (WORKFLOW env var present): full orchestration, new env vars.
  Phase 0 (shadow): existing Step Functions system, old env vars, records
    marked shadow=True.  Lifecycle reconstructed from job.createdAt /
    job.stoppedAt.
"""

import logging
import os
from typing import Any

import boto3

from common import (
    JobChangeEvent,
    JobDetails,
    ProcessingEventRecord,
    ProcessingState,
    S3RecordStore,
)

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


def job_monitor(
    *,
    job_change_event: JobChangeEvent,
    logs_bucket: str,
    retry_queue_url: str,
    failure_dlq_url: str,
) -> None:
    """Handle Batch job state-change events."""
    sqs = boto3.client("sqs")
    details = JobDetails(job_change_event["detail"])
    state = details.get_job_state()

    if details.is_phase1_job():
        event = details.get_granule_event()
        shadow = False
    else:
        event = details.get_shadow_granule_event()
        shadow = True

    logger.info(
        "job_id=%s state=%s workflow=%s shadow=%s",
        details.job_id,
        state.name,
        event.workflow,
        shadow,
    )

    store = S3RecordStore(bucket=logs_bucket)

    terminal_event = ProcessingEventRecord(
        state=state.name,
        ts=details.stopped_at,
        batch_job_id=details.job_id,
        exit_code=details.exit_code,
    )

    for src_id in event.source_granule_ids:
        if shadow:
            # Reconstruct SUBMITTED event from job creation timestamp
            submitted_event = ProcessingEventRecord(
                state=ProcessingState.SUBMITTED.name,
                ts=details.created_at,
                batch_job_id=details.job_id,
            )
            store.append_canonical_event(
                source_granule_id=src_id,
                output_granule_id=event.output_granule_id,
                workflow=event.workflow,
                acquisition_date=event.acquisition_date,
                attempt=event.attempt,
                event=submitted_event,
                batch_job_id=details.job_id,
                shadow=True,
            )

        store.append_canonical_event(
            source_granule_id=src_id,
            output_granule_id=event.output_granule_id,
            workflow=event.workflow,
            acquisition_date=event.acquisition_date,
            attempt=event.attempt,
            event=terminal_event,
            batch_job_id=details.job_id,
            shadow=shadow,
        )
        store.write_state_pointer(
            workflow=event.workflow,
            acquisition_date=event.acquisition_date,
            source_granule_id=src_id,
            attempt=event.attempt,
            new_state=state,
            old_state=ProcessingState.SUBMITTED,
            output_granule_id=event.output_granule_id,
        )

    if state.is_terminal():
        store.write_output_index(
            workflow=event.workflow,
            acquisition_date=event.acquisition_date,
            output_granule_id=event.output_granule_id,
            state=state,
        )

    if not shadow:
        if state == ProcessingState.FAILURE_RETRYABLE:
            if details.job_attempts == details.max_attempts:
                sqs.send_message(
                    QueueUrl=retry_queue_url,
                    MessageBody=event.to_json(),
                    MessageAttributes={
                        "FailureType": {
                            "StringValue": "RETRYABLE",
                            "DataType": "String",
                        }
                    },
                )
            else:
                logger.info(
                    "Retryable failure attempt=%d/%d; AWS Batch will retry internally.",
                    details.job_attempts,
                    details.max_attempts,
                )
        elif state == ProcessingState.FAILURE_NONRETRYABLE:
            sqs.send_message(
                QueueUrl=failure_dlq_url,
                MessageBody=event.to_json(),
                MessageAttributes={
                    "FailureType": {"StringValue": "NONRETRYABLE", "DataType": "String"}
                },
            )


def handler(event: JobChangeEvent, context: Any) -> None:
    """Lambda entry point for AWS Batch job state change events."""
    job_monitor(
        job_change_event=event,
        logs_bucket=os.environ["PROCESSING_BUCKET_NAME"],
        retry_queue_url=os.environ["JOB_RETRY_QUEUE_URL"],
        failure_dlq_url=os.environ["JOB_FAILURE_DLQ_URL"],
    )
