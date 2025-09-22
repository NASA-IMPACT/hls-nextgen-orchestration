"""Tests for `job_monitor` Lambda"""

import pytest
from mypy_boto3_batch.type_defs import JobDetailTypeDef
from mypy_boto3_sqs import SQSClient

from common import GranuleId, JobOutcome, ProcessingOutcome
from common.aws_batch import JobChangeEvent, JobDetails
from common.granule_logger import GranuleLoggerService
from job_monitor.handler import job_monitor


@pytest.fixture
def job_logger(bucket: str) -> GranuleLoggerService:
    return GranuleLoggerService(bucket, "logs")


def test_handler_logs_nonretryable_failure(
    granule_id: GranuleId,
    job_logger: GranuleLoggerService,
    sqs: SQSClient,
    retry_queue: str,
    failure_dlq: str,
    event_job_detail_change_failed: JobChangeEvent,
) -> None:
    """Test the handler"""
    event = event_job_detail_change_failed.copy()
    event["detail"]["container"]["exitCode"] = 1
    assert (
        JobDetails(event["detail"]).get_job_outcome() == JobOutcome.FAILURE_NONRETRYABLE
    )

    job_monitor(
        job_change_event=event,
        logs_bucket=job_logger.bucket,
        logs_prefix=job_logger.logs_prefix,
        retry_queue_url=retry_queue,
        failure_dlq_url=failure_dlq,
    )

    messages = sqs.receive_message(QueueUrl=failure_dlq)["Messages"]
    assert len(messages) == 1

    events = job_logger.list_events(granule_id)
    assert len(events[ProcessingOutcome.FAILURE]) == 1
    assert events[ProcessingOutcome.FAILURE][0].attempt == 0
    assert ProcessingOutcome.SUCCESS not in events


def test_handler_retryable_failure_last_attempt(
    granule_id: GranuleId,
    job_logger: GranuleLoggerService,
    sqs: SQSClient,
    retry_queue: str,
    failure_dlq: str,
    event_job_detail_change_failed: JobChangeEvent,
    job_detail_failed_spot: JobDetailTypeDef,
) -> None:
    """Test behavior for Spot failure on last AWS Batch attempt

    The handler should log the attempt AND requeue it since AWS Batch won't
    retry it internally.
    """
    event = event_job_detail_change_failed.copy()
    event["detail"] = job_detail_failed_spot
    event["detail"]["attempts"] = [event["detail"]["attempts"][0]] * 3

    job_monitor(
        job_change_event=event,
        logs_bucket=job_logger.bucket,
        logs_prefix=job_logger.logs_prefix,
        retry_queue_url=retry_queue,
        failure_dlq_url=failure_dlq,
    )

    messages = sqs.receive_message(QueueUrl=retry_queue)["Messages"]
    assert len(messages) == 1

    events = job_logger.list_events(granule_id)
    assert len(events[ProcessingOutcome.FAILURE]) == 1
    assert events[ProcessingOutcome.FAILURE][0].attempt == 0
    assert ProcessingOutcome.SUCCESS not in events


def test_handler_retryable_failure_doesnt_requeue_nonfinal_attempt(
    granule_id: GranuleId,
    job_logger: GranuleLoggerService,
    sqs: SQSClient,
    retry_queue: str,
    failure_dlq: str,
    event_job_detail_change_failed: JobChangeEvent,
    job_detail_failed_spot: JobDetailTypeDef,
) -> None:
    """Test behavior for Spot failure on last AWS Batch attempt

    The handler should log the attempt and does NOT requeue it since AWS Batch will
    retry it internally.
    """
    event = event_job_detail_change_failed.copy()
    event["detail"] = job_detail_failed_spot
    event["detail"]["attempts"] = [event["detail"]["attempts"][0]] * 1

    job_monitor(
        job_change_event=event,
        logs_bucket=job_logger.bucket,
        logs_prefix=job_logger.logs_prefix,
        retry_queue_url=retry_queue,
        failure_dlq_url=failure_dlq,
    )

    messages = sqs.receive_message(QueueUrl=retry_queue).get("Messages", [])
    assert len(messages) == 0

    events = job_logger.list_events(granule_id)
    assert len(events[ProcessingOutcome.FAILURE]) == 1
    assert ProcessingOutcome.SUCCESS not in events


def test_handler_logs_success(
    granule_id: GranuleId,
    job_logger: GranuleLoggerService,
    sqs: SQSClient,
    retry_queue: str,
    failure_dlq: str,
    event_job_detail_change_failed: JobChangeEvent,
) -> None:
    """Test the handler"""
    event = event_job_detail_change_failed.copy()
    event["detail"]["container"]["exitCode"] = 0
    job_monitor(
        job_change_event=event,
        logs_bucket=job_logger.bucket,
        logs_prefix=job_logger.logs_prefix,
        retry_queue_url=retry_queue,
        failure_dlq_url=failure_dlq,
    )

    events = job_logger.list_events(granule_id)
    assert len(events[ProcessingOutcome.SUCCESS]) == 1
    assert events[ProcessingOutcome.SUCCESS][0].attempt == 0
    assert ProcessingOutcome.FAILURE not in events
