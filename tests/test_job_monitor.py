"""Tests for the job_monitor Lambda."""

import json

import pytest
from mypy_boto3_sqs import SQSClient

from common import ProcessingState, S3RecordStore
from common.aws_batch import JobChangeEvent
from job_monitor.handler import job_monitor


@pytest.fixture
def store(bucket: str) -> S3RecordStore:
    return S3RecordStore(bucket=bucket)


def _run(
    event: JobChangeEvent, bucket: str, retry_queue: str, failure_dlq: str
) -> None:
    job_monitor(
        job_change_event=event,
        logs_bucket=bucket,
        retry_queue_url=retry_queue,
        failure_dlq_url=failure_dlq,
    )


# ---------------------------------------------------------------------------
# Phase 1 (new orchestration, WORKFLOW env var present)
# ---------------------------------------------------------------------------


class TestPhase1:
    def test_success_writes_canonical_and_pointer(
        self,
        event_job_success: JobChangeEvent,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue: str,
        failure_dlq: str,
    ) -> None:
        _run(event_job_success, store.bucket, retry_queue, failure_dlq)

        from tests.conftest import ACQUISITION_DATE, SAFE_ID

        key = S3RecordStore.canonical_key("sentinel", ACQUISITION_DATE, SAFE_ID, 0)
        s3 = __import__("boto3").client("s3", region_name="us-west-2")
        record = json.loads(s3.get_object(Bucket=store.bucket, Key=key)["Body"].read())
        assert record["current_state"] == "SUCCESS"
        assert len(record["events"]) == 1
        assert record["shadow"] is False

    def test_success_writes_output_index(
        self,
        event_job_success: JobChangeEvent,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue: str,
        failure_dlq: str,
    ) -> None:
        _run(event_job_success, store.bucket, retry_queue, failure_dlq)

        from tests.conftest import ACQUISITION_DATE, GRANULE_ID_STR

        key = S3RecordStore.output_index_key(
            ProcessingState.SUCCESS, "sentinel", ACQUISITION_DATE, GRANULE_ID_STR
        )
        s3 = __import__("boto3").client("s3", region_name="us-west-2")
        resp = s3.list_objects_v2(Bucket=store.bucket, Prefix=key)
        assert resp.get("KeyCount", 0) == 1

    def test_cloudy_writes_terminal_and_output_index(
        self,
        event_job_cloudy: JobChangeEvent,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue: str,
        failure_dlq: str,
    ) -> None:
        _run(event_job_cloudy, store.bucket, retry_queue, failure_dlq)

        from tests.conftest import ACQUISITION_DATE, GRANULE_ID_STR, SAFE_ID

        key = S3RecordStore.canonical_key("sentinel", ACQUISITION_DATE, SAFE_ID, 0)
        s3 = __import__("boto3").client("s3", region_name="us-west-2")
        record = json.loads(s3.get_object(Bucket=store.bucket, Key=key)["Body"].read())
        assert record["current_state"] == "CLOUDY"

        idx_key = S3RecordStore.output_index_key(
            ProcessingState.CLOUDY, "sentinel", ACQUISITION_DATE, GRANULE_ID_STR
        )
        resp = s3.list_objects_v2(Bucket=store.bucket, Prefix=idx_key)
        assert resp.get("KeyCount", 0) == 1

        # Not routed to any queue
        assert not sqs.receive_message(QueueUrl=retry_queue).get("Messages")
        assert not sqs.receive_message(QueueUrl=failure_dlq).get("Messages")

    def test_low_sun_angle(
        self,
        event_job_low_sun: JobChangeEvent,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue: str,
        failure_dlq: str,
    ) -> None:
        _run(event_job_low_sun, store.bucket, retry_queue, failure_dlq)

        from tests.conftest import ACQUISITION_DATE, SAFE_ID

        key = S3RecordStore.canonical_key("sentinel", ACQUISITION_DATE, SAFE_ID, 0)
        s3 = __import__("boto3").client("s3", region_name="us-west-2")
        record = json.loads(s3.get_object(Bucket=store.bucket, Key=key)["Body"].read())
        assert record["current_state"] == "LOW_SUN_ANGLE"

    def test_nonretryable_routes_to_dlq(
        self,
        event_job_failed_error: JobChangeEvent,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue: str,
        failure_dlq: str,
    ) -> None:
        _run(event_job_failed_error, store.bucket, retry_queue, failure_dlq)
        msgs = sqs.receive_message(QueueUrl=failure_dlq).get("Messages", [])
        assert len(msgs) == 1

    def test_retryable_last_attempt_routes_to_retry_queue(
        self,
        event_job_failed_spot: JobChangeEvent,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue: str,
        failure_dlq: str,
    ) -> None:
        _run(event_job_failed_spot, store.bucket, retry_queue, failure_dlq)
        msgs = sqs.receive_message(QueueUrl=retry_queue).get("Messages", [])
        assert len(msgs) == 1

    def test_retryable_nonfinal_attempt_not_requeued(
        self,
        event_job_failed_spot_nonfinal: JobChangeEvent,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue: str,
        failure_dlq: str,
    ) -> None:
        _run(event_job_failed_spot_nonfinal, store.bucket, retry_queue, failure_dlq)
        msgs = sqs.receive_message(QueueUrl=retry_queue).get("Messages", [])
        assert len(msgs) == 0


# ---------------------------------------------------------------------------
# Phase 0 (shadow mode, no WORKFLOW env var)
# ---------------------------------------------------------------------------


class TestPhase0Shadow:
    def test_shadow_sentinel_writes_two_events(
        self,
        event_job_shadow_sentinel: JobChangeEvent,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue: str,
        failure_dlq: str,
    ) -> None:
        _run(event_job_shadow_sentinel, store.bucket, retry_queue, failure_dlq)

        s3 = __import__("boto3").client("s3", region_name="us-west-2")
        # Find canonical record — we don't know the exact acquisition_date easily
        # so just list all canonical records and check there is one
        resp = s3.list_objects_v2(Bucket=store.bucket, Prefix="records/sentinel/")
        assert resp.get("KeyCount", 0) == 1

        key = resp["Contents"][0]["Key"]
        record = json.loads(s3.get_object(Bucket=store.bucket, Key=key)["Body"].read())
        # Shadow records get two events: SUBMITTED (from createdAt) + terminal
        assert len(record["events"]) == 2
        assert record["events"][0]["state"] == "SUBMITTED"
        assert record["shadow"] is True
        assert record["current_state"] == "SUCCESS"
