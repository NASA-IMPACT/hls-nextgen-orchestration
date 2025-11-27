"""Marks granules as awaiting auxiliary data or submits them."""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext

from common import (
    GranuleProcessingEvent,
    ProcessingState,
)
from common.granule_logger import GranuleLoggerService

if TYPE_CHECKING:
    from typing import Any

logger = Logger()
tracer = Tracer()
metrics = Metrics()


@tracer.capture_method
def parse_s3_sns_message(sqs_body: str) -> list[dict]:
    """Parse S3 event notification from SNS message wrapped in SQS.

    Args:
        sqs_body: The SQS message body containing an SNS notification

    Returns:
        List of S3 event records from the notification
    """
    sns_message = json.loads(sqs_body)

    s3_event = json.loads(sns_message["Message"])

    return s3_event.get("Records", [])  # type: ignore


@tracer.capture_method
def extract_granule_id_from_s3_key(s3_key: str) -> str:
    """Extract granule ID from S3 object key.

    Args:
        s3_key: The S3 object key

    Returns:
        The granule ID extracted from the key
    """
    filename = os.path.basename(s3_key)

    granule_id = os.path.splitext(filename)[0]

    return granule_id


@tracer.capture_method
def process_record(sqs_body: str) -> None:
    """Process a single SQS message body containing S3 event notifications.

    Args:
        sqs_body: SQS message body containing SNS-wrapped S3 event notifications
    """
    logs_bucket = os.environ["PROCESSING_BUCKET_NAME"]
    logs_prefix = os.environ.get("PROCESSING_BUCKET_LOG_PREFIX", "logs")

    granule_logger = GranuleLoggerService(
        bucket=logs_bucket,
        logs_prefix=logs_prefix,
    )

    s3_records = parse_s3_sns_message(sqs_body)

    for s3_record in s3_records:
        s3_info = s3_record["s3"]
        bucket_name = s3_info["bucket"]["name"]
        object_key = s3_info["object"]["key"]

        granule_id = extract_granule_id_from_s3_key(object_key)

        granule_event = GranuleProcessingEvent(
            granule_id=granule_id,
            attempt=0,
        )

        logger.info(
            "Marking granule as AWAITING",
            extra={
                "granule_id": granule_event.granule_id,
                "s3_bucket": bucket_name,
                "s3_key": object_key,
            },
        )
        granule_logger.put_event(
            event=granule_event,
            state=ProcessingState.AWAITING,
        )

        metrics.add_metric(name="GranulesProcessed", unit="Count", value=1)


@logger.inject_lambda_context
@tracer.capture_lambda_handler
@metrics.log_metrics
def handler(event: dict[str, Any], context: LambdaContext) -> None:
    """Lambda handler for processing a single S3 event notification.

    Args:
        event: SQS event containing a single SNS-wrapped S3 notification
        context: Lambda context object
    """
    # Extract the single SQS record from the event
    records = event.get("Records", [])
    if not records:
        logger.warning("No records found in event")
        return

    if len(records) > 1:
        logger.warning(
            "Multiple records received, only processing the first",
            extra={"record_count": len(records)},
        )

    # Process only the first record
    record = records[0]
    sqs_body = record["body"]

    process_record(sqs_body)
