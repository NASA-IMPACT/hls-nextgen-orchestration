"""HLS job resubmit Lambda.

Drains the retry queue BEJM's job monitor routes retryable failures to, and
resubmits each job group's next attempt with the same container environment
the submission Lambdas set.
"""

from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING, Any

import boto3
from batch_event_job_monitor import JobGroup, RetryMessage, resubmit_job
from batch_event_job_monitor.models import JobTypeConfig

from common.jobs import build_submit_job_params

if TYPE_CHECKING:
    from aws_lambda_typing.context import Context
    from aws_lambda_typing.events import SQSEvent

logger = logging.getLogger(__name__)

_batch_client = boto3.client("batch")


def _build_submit_job_params(job_group: JobGroup) -> dict[str, Any]:
    configs = json.loads(os.environ["PROCESSING_JOB_TYPE_CONFIGS"])
    config = JobTypeConfig.from_dict(configs[job_group.job_type])
    return build_submit_job_params(
        job_group,
        job_queue=config.job_queue_arn,
        job_definition=config.job_definition_arn,
        output_bucket=os.environ["OUTPUT_BUCKET_NAME"],
    )


def handler(event: SQSEvent, context: Context) -> dict[str, list[dict[str, str]]]:
    """Resubmit each failed job on the retry queue, reporting partial failures."""
    batch_item_failures: list[dict[str, str]] = []
    for record in event["Records"]:
        try:
            message = RetryMessage.from_json(record["body"])
            resubmit_job(
                batch_client=_batch_client,
                build_submit_job_params=_build_submit_job_params,
                job_group=message.job_group,
            )
        except Exception:
            logger.exception("Failed to resubmit message %s", record["messageId"])
            batch_item_failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": batch_item_failures}
