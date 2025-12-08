# ruff: noqa: E501
"""Tests for `granule_entry` Lambda"""

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from aws_lambda_powertools.utilities.typing import LambdaContext

from common import GranuleId, ProcessingState
from common.granule_logger import GranuleLoggerService
from granule_entry.handler import (
    convert_safe_id_to_hls_id,
    extract_safe_id_from_s3_key,
    handler,
    parse_s3_sns_message,
    process_record,
)


@pytest.fixture
def lambda_context() -> LambdaContext:
    """Mock Lambda context"""
    context = MagicMock(spec=LambdaContext)
    context.function_name = "test-granule-entry"
    context.memory_limit_in_mb = 128
    context.invoked_function_arn = (
        "arn:aws:lambda:us-west-2:123456789012:function:test-granule-entry"
    )
    context.aws_request_id = "test-request-id"
    return context


@pytest.fixture
def granule_logger(bucket: str) -> GranuleLoggerService:
    return GranuleLoggerService(bucket, "logs")


@pytest.fixture
def s3_event_notification() -> dict[str, Any]:
    """S3 event notification that would be inside the SNS message"""
    return {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {
                        "name": "test-bucket",
                        "arn": "arn:aws:s3:::test-bucket",
                    },
                    "object": {
                        "key": "input/S2A_MSIL1C_20230817T154921_N0509_R011_T18TYN_20230817T204510.zip",
                        "size": 1024,
                    },
                },
            }
        ]
    }


@pytest.fixture
def sns_message(s3_event_notification: dict[str, Any]) -> dict[str, Any]:
    """SNS message containing S3 event notification"""
    return {
        "Type": "Notification",
        "MessageId": "test-message-id",
        "TopicArn": "arn:aws:sns:us-west-2:123456789012:test-topic",
        "Subject": "Amazon S3 Notification",
        "Message": json.dumps(s3_event_notification),
        "Timestamp": "2022-08-14T21:49:21.000Z",
    }


@pytest.fixture
def sqs_event(sns_message: dict[str, Any]) -> dict[str, Any]:
    """SQS event containing SNS-wrapped S3 event notification"""
    return {
        "Records": [
            {
                "messageId": "test-message-1",
                "receiptHandle": "test-receipt-handle-1",
                "body": json.dumps(sns_message),
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1660510161000",
                    "SenderId": "test-sender",
                    "ApproximateFirstReceiveTimestamp": "1660510161000",
                },
                "messageAttributes": {},
                "md5OfBody": "test-md5",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-west-2:123456789012:test-queue",
                "awsRegion": "us-west-2",
            }
        ]
    }


@pytest.fixture
def sqs_event_multiple_granules(sns_message: dict[str, Any]) -> dict[str, Any]:
    """SQS event with multiple S3 objects in a single SNS message"""
    s3_event_with_multiple = {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": "test-bucket"},
                    "object": {
                        "key": "input/S2A_MSIL1C_20230817T154921_N0509_R011_T18TYN_20230817T204510.zip"
                    },
                },
            },
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": "test-bucket"},
                    "object": {
                        "key": "input/S2A_MSIL1C_20230817T154921_N0509_R011_T19TYN_20230817T204510.zip"
                    },
                },
            },
        ]
    }

    sns_message_multi = sns_message.copy()
    sns_message_multi["Message"] = json.dumps(s3_event_with_multiple)

    return {
        "Records": [
            {
                "messageId": "test-message-1",
                "receiptHandle": "test-receipt-handle-1",
                "body": json.dumps(sns_message_multi),
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1660510161000",
                },
                "messageAttributes": {},
                "md5OfBody": "test-md5",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-west-2:123456789012:test-queue",
                "awsRegion": "us-west-2",
            }
        ]
    }


def test_parse_s3_sns_message(sns_message: dict[str, Any]) -> None:
    """Test parsing S3 event from SNS message"""
    sqs_body = json.dumps(sns_message)

    result = parse_s3_sns_message(sqs_body)

    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["eventSource"] == "aws:s3"
    assert result[0]["s3"]["bucket"]["name"] == "test-bucket"
    assert (
        result[0]["s3"]["object"]["key"]
        == "input/S2A_MSIL1C_20230817T154921_N0509_R011_T18TYN_20230817T204510.zip"
    )


def test_parse_s3_sns_message_multiple_records(
    s3_event_notification: dict[str, Any],
) -> None:
    """Test parsing multiple S3 events from a single SNS message"""
    # Add a second S3 record
    s3_event_notification["Records"].append(
        {
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "s3": {
                "bucket": {"name": "test-bucket-2"},
                "object": {
                    "key": "input/S2A_MSIL1C_20230817T154921_N0509_R011_T18TYN_20230817T204510.zip"
                },
            },
        }
    )

    sns_message = {
        "Type": "Notification",
        "Message": json.dumps(s3_event_notification),
    }
    sqs_body = json.dumps(sns_message)

    result = parse_s3_sns_message(sqs_body)

    assert len(result) == 2
    assert result[0]["s3"]["bucket"]["name"] == "test-bucket"
    assert result[1]["s3"]["bucket"]["name"] == "test-bucket-2"


def test_parse_s3_sns_message_empty_records() -> None:
    """Test parsing SNS message with no S3 records"""
    s3_event = {"Records": []}  # type: ignore
    sns_message = {"Type": "Notification", "Message": json.dumps(s3_event)}
    sqs_body = json.dumps(sns_message)

    result = parse_s3_sns_message(sqs_body)

    assert result == []


def test_convert_safe_id_to_granule_id() -> None:
    safe_id = "S2A_MSIL1C_20230817T154921_N0509_R011_T18TYN_20230817T204510"
    result = convert_safe_id_to_hls_id(safe_id)
    assert result == "HLS.S30.T18TYN.2023229T154921.v2.0"


def test_extract_safe_id_from_s3_key() -> None:
    """Test extracting granule ID from S3 key"""
    s3_key = "input/S2A_MSIL1C_20230817T154921_N0509_R011_T18TYN_20230817T204510.zip"

    result = extract_safe_id_from_s3_key(s3_key)

    assert result == "S2A_MSIL1C_20230817T154921_N0509_R011_T18TYN_20230817T204510"


def test_extract_safe_id_from_s3_key_nested_path() -> None:
    """Test extracting granule ID from deeply nested S3 path"""
    s3_key = "data/2023/08/17/S2A_MSIL1C_20230817T154921_N0509_R011_T18TYN_20230817T204510.zip"

    result = extract_safe_id_from_s3_key(s3_key)

    assert result == "S2A_MSIL1C_20230817T154921_N0509_R011_T18TYN_20230817T204510"


def test_process_record(
    bucket: str,
    granule_logger: GranuleLoggerService,
    granule_id: GranuleId,
    source_granule_id: str,
    sns_message: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test processing a single SQS message body"""
    monkeypatch.setenv("PROCESSING_BUCKET_NAME", bucket)
    monkeypatch.setenv("PROCESSING_BUCKET_LOG_PREFIX", "logs")

    # Process the record with SQS body
    sqs_body = json.dumps(sns_message)
    process_record(sqs_body)

    # Verify the event was logged
    events = granule_logger.list_events(
        granule_id=str(granule_id),
        source_granule_id=source_granule_id,
    )

    assert ProcessingState.AWAITING in events
    assert len(events[ProcessingState.AWAITING]) == 1
    assert events[ProcessingState.AWAITING][0].granule_id == str(granule_id)
    assert events[ProcessingState.AWAITING][0].attempt == 0


def test_process_record_multiple_s3_events(
    bucket: str,
    granule_logger: GranuleLoggerService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test processing SQS message with multiple S3 events"""
    monkeypatch.setenv("PROCESSING_BUCKET_NAME", bucket)

    safe_id_1 = "S2A_MSIL1C_20230817T154921_N0509_R011_T18TYN_20230817T204510"
    safe_id_2 = "S2A_MSIL1C_20230817T154921_N0509_R011_T19TYN_20230817T204510"

    s3_event_with_multiple = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "test-bucket"},
                    "object": {"key": f"input/{safe_id_1}.zip"},
                }
            },
            {
                "s3": {
                    "bucket": {"name": "test-bucket"},
                    "object": {"key": f"input/{safe_id_2}.zip"},
                }
            },
        ]
    }

    sns_message = {"Message": json.dumps(s3_event_with_multiple)}
    sqs_body = json.dumps(sns_message)

    # Process the record
    process_record(sqs_body)

    # Verify both granules were logged
    granule_id_1 = GranuleId.from_str("HLS.S30.T18TYN.2023229T154921.v2.0")
    granule_id_2 = GranuleId.from_str("HLS.S30.T19TYN.2023229T154921.v2.0")

    events_1 = granule_logger.list_events(
        granule_id=granule_id_1,
        source_granule_id=safe_id_1,
    )
    events_2 = granule_logger.list_events(
        granule_id=granule_id_2,
        source_granule_id=safe_id_2,
    )

    assert ProcessingState.AWAITING in events_1
    assert ProcessingState.AWAITING in events_2
    assert len(events_1[ProcessingState.AWAITING]) == 1
    assert len(events_2[ProcessingState.AWAITING]) == 1


def test_handler(
    bucket: str,
    granule_id: GranuleId,
    source_granule_id: str,
    granule_logger: GranuleLoggerService,
    sqs_event: dict[str, Any],
    lambda_context: LambdaContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test the Lambda handler function with a single record"""
    monkeypatch.setenv("PROCESSING_BUCKET_NAME", bucket)
    monkeypatch.setenv("PROCESSING_BUCKET_LOG_PREFIX", "logs")

    result = handler(sqs_event, lambda_context)

    # Handler now returns None
    assert result is None

    # Verify the granule was logged
    events = granule_logger.list_events(
        granule_id=str(granule_id),
        source_granule_id=source_granule_id,
    )

    assert ProcessingState.AWAITING in events
    assert len(events[ProcessingState.AWAITING]) == 1


def test_handler_with_multiple_s3_records(
    bucket: str,
    granule_logger: GranuleLoggerService,
    sqs_event_multiple_granules: dict[str, Any],
    lambda_context: LambdaContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test handler processing multiple S3 records within a single SQS message"""
    monkeypatch.setenv("PROCESSING_BUCKET_NAME", bucket)

    result = handler(sqs_event_multiple_granules, lambda_context)

    assert result is None

    safe_id_1 = "S2A_MSIL1C_20230817T154921_N0509_R011_T18TYN_20230817T204510"
    safe_id_2 = "S2A_MSIL1C_20230817T154921_N0509_R011_T19TYN_20230817T204510"

    # Verify both granules were logged (both S3 records in the single SNS message)
    granule_id_1 = GranuleId.from_str("HLS.S30.T18TYN.2023229T154921.v2.0")
    granule_id_2 = GranuleId.from_str("HLS.S30.T19TYN.2023229T154921.v2.0")

    events_1 = granule_logger.list_events(
        granule_id=str(granule_id_1),
        source_granule_id=safe_id_1,
    )
    events_2 = granule_logger.list_events(
        granule_id=str(granule_id_2),
        source_granule_id=safe_id_2,
    )

    assert ProcessingState.AWAITING in events_1
    assert ProcessingState.AWAITING in events_2


@patch("granule_entry.handler.process_record")
def test_handler_processing_failure(
    mock_process_record: MagicMock,
    bucket: str,
    sqs_event: dict[str, Any],
    lambda_context: LambdaContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test handler with processing failure"""
    monkeypatch.setenv("PROCESSING_BUCKET_NAME", bucket)

    # Make process_record raise an exception
    mock_process_record.side_effect = Exception("Processing failed")

    # Should raise the exception since we're not doing batch processing
    with pytest.raises(Exception, match="Processing failed"):
        handler(sqs_event, lambda_context)


def test_handler_empty_event(
    bucket: str,
    lambda_context: LambdaContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test handler with empty SQS event"""
    monkeypatch.setenv("PROCESSING_BUCKET_NAME", bucket)

    empty_event = {"Records": []}  # type: ignore

    result = handler(empty_event, lambda_context)

    assert result is None


def test_handler_multiple_sqs_records(
    bucket: str,
    granule_logger: GranuleLoggerService,
    granule_id: GranuleId,
    source_granule_id: str,
    sns_message: dict[str, Any],
    lambda_context: LambdaContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test handler with multiple SQS records (should only process first)"""
    monkeypatch.setenv("PROCESSING_BUCKET_NAME", bucket)

    # Create an event with multiple SQS records
    multi_record_event = {
        "Records": [
            {
                "messageId": "test-message-1",
                "body": json.dumps(sns_message),
            },
            {
                "messageId": "test-message-2",
                "body": json.dumps(sns_message),
            },
        ]
    }

    result = handler(multi_record_event, lambda_context)

    assert result is None

    # Verify only one granule was logged (from the first record)
    events = granule_logger.list_events(
        granule_id=granule_id,
        source_granule_id=source_granule_id,
    )

    assert ProcessingState.AWAITING in events
    # Should only be one event logged (not two, even though there were two SQS records)
    assert len(events[ProcessingState.AWAITING]) == 1
