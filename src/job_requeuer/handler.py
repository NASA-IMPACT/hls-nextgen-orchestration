"""HLS-VI historical processing job requeuer."""

from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING

from common import (
    AwsBatchClient,
    GranuleProcessingEvent,
)

if TYPE_CHECKING:
    from aws_lambda_typing.context import Context
    from aws_lambda_typing.events import SQSEvent

logger = logging.getLogger(__name__)
if logger.hasHandlers():
    logger.setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)


def job_requeuer(
    job_queue: str,
    job_definition_name: str,
    output_bucket: str,
    event: SQSEvent,
) -> list[str]:
    """Requeue granule processing events"""
    batch = AwsBatchClient(queue=job_queue, job_definition=job_definition_name)

    jobs = []
    for record in event["Records"]:
        failed_event = GranuleProcessingEvent(**json.loads(record["body"]))
        next_attempt = failed_event.new_attempt()

        logger.info(f"Submitting job for {next_attempt}")
        job_id = batch.submit_job(
            event=next_attempt,
            output_bucket=output_bucket,
        )
        jobs.append(job_id)
    return jobs


def handler(event: SQSEvent, context: Context) -> dict[str, list[str]]:
    """Resubmit failed processing events that can be retried

    This Lambda is fed by a SQS queue, so the event payload looks like,
    https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html#example-standard-queue-message-event

    The body of each event looks like,
    ```
    {
        "GRANULE_ID": "HLS.S30.T01GEL.2019059T213751.v2.0",
        "ATTEMPT": 0
    }
    ```
    """
    job_queue = os.environ["BATCH_QUEUE_NAME"]
    job_definition_name = os.environ["BATCH_JOB_DEFINITION_NAME"]
    output_bucket = os.environ["OUTPUT_BUCKET"]
    debug_bucket = os.environ.get("DEBUG_BUCKET")

    job_requeuer(
        job_queue=job_queue,
        job_definition_name=job_definition_name,
        output_bucket=debug_bucket or output_bucket,
        event=event,
    )
    return {"batchItemFailures": []}
