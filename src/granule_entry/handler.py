"""Marks granules as awaiting auxiliary data or submits them."""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import TYPE_CHECKING

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext

from common import (
    AwsBatchClient,
    GranuleId,
    GranuleProcessingEvent,
    ProcessingState,
)
from common.granule_logger import GranuleLoggerService

if TYPE_CHECKING:
    from typing import Any

logger = Logger()
tracer = Tracer()
metrics = Metrics(namespace="hls_granule_entry")


@tracer.capture_method
def submit_job(
    granule_id: str,
    source_granule_id: str,
    granule_logger: GranuleLoggerService,
) -> None:
    """Submit granule processing jobs to AWS Batch queue"""
    output_bucket = os.environ["FMASK_OUTPUT_BUCKET_NAME"]
    job_queue = os.environ["BATCH_QUEUE_NAME"]
    job_definition_name = os.environ["FMASK_JOB_DEFINITION_NAME"]
    max_active_jobs = int(os.environ["MAX_ACTIVE_JOBS"])
    # debug_bucket = os.environ.get("DEBUG_BUCKET")
    batch = AwsBatchClient(queue=job_queue, job_definition=job_definition_name)

    if not batch.active_jobs_below_threshold(max_active_jobs):
        logger.info("Too many active jobs in AWS Batch cluster, exiting early")
        return

    processing_event = GranuleProcessingEvent(
        granule_id=granule_id,
        attempt=0,
        source_granule_id=source_granule_id,
    )
    batch.submit_job(
        event=processing_event,
        output_bucket=output_bucket,
    )
    granule_logger.put_event(
        event=processing_event,
        state=ProcessingState.SUBMITTED,
    )
    logger.info(f"Submitted granule {granule_id} for Fmask")


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
def extract_safe_id_from_s3_key(s3_key: str) -> str:
    """Extract SAFE ID from S3 object key.

    Args:
        s3_key: The S3 object key

    Returns:
        The safe ID extracted from the key
    """
    filename = os.path.basename(s3_key)

    safe_id = os.path.splitext(filename)[0]

    return safe_id


@tracer.capture_method
def convert_safe_id_to_hls_id(safe_id: str) -> str:
    """Convert a SAFE id to an HLS granule id.

    Args:
        safe_id: A SAFE id

    Returns:
        The HLS granule id
    """
    safe_components = safe_id.split("_")

    date_str = safe_components[2][:15]

    year = date_str[0:4]
    month = date_str[4:6]
    day = date_str[6:8]
    hms = date_str[8:15]

    dt = datetime.strptime(f"{year}{month}{day}", "%Y%m%d")
    day_of_year = f"{dt.timetuple().tm_yday:03d}"  # zero-padded

    hlsversion = "v2.0"

    tile = safe_components[5]

    granule_id = f"HLS.S30.{tile}.{year}{day_of_year}{hms}.{hlsversion}"

    return granule_id


@tracer.capture_method
def key_pattern_exists(bucket: str, pattern: str) -> bool:
    """Check if any S3 keys exist that match the given pattern.

    Args:
        bucket: S3 bucket name
        pattern: S3 key prefix pattern to search for

    Returns:
        bool: True if at least one matching key exists
    """
    s3_client = boto3.client("s3")

    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=pattern, MaxKeys=1)
        return bool(response.get("KeyCount", 0) > 0)
    except Exception as e:
        logger.error(
            "Error checking S3 key pattern",
            extra={"bucket": bucket, "pattern": pattern, "error": str(e)},
        )
        return False


@tracer.capture_method
def check_aux_data(granule_id: GranuleId) -> bool:
    """Checks an S3 bucket to determine if auxiliary data exists for granule

    Args:
        granule_id: A granule id.

    Returns:
        bool: True if auxiliary data exists for the granule date
    """
    bucket = os.environ.get("AUX_DATA_BUCKET_NAME")
    if not bucket:
        logger.error("AUX_DATA_BUCKET_NAME environment variable not set")
        return False

    year = granule_id.begin_datetime.strftime("%Y")
    ydoy = granule_id.begin_datetime.strftime("%Y%j")

    vj_pattern = f"lasrc_aux/LADS/{year}/VJ104ANC.A{ydoy}"
    vj_exists = key_pattern_exists(bucket, vj_pattern)

    vnp_pattern = f"lasrc_aux/LADS/{year}/VNP04ANC.A{ydoy}"
    vnp_exists = key_pattern_exists(bucket, vnp_pattern)

    return bool(vj_exists or vnp_exists)


@tracer.capture_method
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

        safe_id = extract_safe_id_from_s3_key(object_key)
        granule_id_str = convert_safe_id_to_hls_id(safe_id)
        granule_id = GranuleId.from_str(granule_id_str)

        granule_event = GranuleProcessingEvent(
            granule_id=granule_id_str,
            source_granule_id=safe_id,
            attempt=0,
        )

        aux_data = check_aux_data(granule_id)
        if aux_data:
            submit_job(
                granule_id=granule_id_str,
                source_granule_id=safe_id,
                granule_logger=granule_logger,
            )
        else:
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
