"""Tests for the job monitor Lambda: BEJM's handler with the Phase 0 resolver."""

import json
from typing import Any

import pytest
from batch_event_job_monitor import JobContext, S3RecordStore
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient

from common.jobs import (
    PHASE0_SENTINEL,
    SENTINEL,
    SUBMITTED,
    phase0_job_type_config,
    sentinel_job_group,
    sentinel_job_type_config,
)
from tests.conftest import ACQUISITION_DATE, GRANULE_ID_STR, SAFE_ID, TWIN_SAFE_ID

_QUEUE = "arn:aws:batch:us-west-2:123456789012:job-queue/hls-orch-dev"
_JOB_DEFINITION = "arn:aws:batch:us-west-2:123456789012:job-definition/sentinel-ac"
_PHASE0_QUEUE = "arn:aws:batch:us-west-2:123456789012:job-queue/phase0"
_PHASE0_JOB_DEFINITION = "arn:aws:batch:us-west-2:123456789012:job-definition/old"

JOB_GROUP = sentinel_job_group(
    acquisition_date=ACQUISITION_DATE,
    source_granule_ids=[SAFE_ID, TWIN_SAFE_ID],
    output_granule_id=GRANULE_ID_STR,
)


@pytest.fixture
def handler(
    bucket: str,
    retry_queue: str,
    failure_dlq: str,
    monkeypatch: pytest.MonkeyPatch,
) -> Any:
    configs = {
        SENTINEL: sentinel_job_type_config(
            job_queue_arn=_QUEUE, job_definition_arn=_JOB_DEFINITION, max_attempts=3
        ),
        PHASE0_SENTINEL: phase0_job_type_config(
            job_queue_arn=_PHASE0_QUEUE, job_definition_arn=_PHASE0_JOB_DEFINITION
        ),
    }
    monkeypatch.setenv(
        "PROCESSING_JOB_TYPE_CONFIGS",
        json.dumps({job_type: c.to_dict() for job_type, c in configs.items()}),
    )
    from job_monitor.handler import handler

    return handler


def _event(
    status: str,
    *,
    job_id: str = "job-1",
    exit_code: int | None = None,
    parameters: dict[str, str] | None = None,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    container: dict[str, Any] = {
        "environment": [{"name": k, "value": v} for k, v in (env or {}).items()]
    }
    if exit_code is not None:
        container["exitCode"] = exit_code
    return {
        "time": "2025-04-23T19:01:54Z",
        "detail": {
            "jobId": job_id,
            "jobName": "test-job",
            "jobQueue": _QUEUE if parameters else _PHASE0_QUEUE,
            "jobDefinition": f"{_JOB_DEFINITION}:3",
            "status": status,
            "createdAt": 1745434620000,
            "parameters": parameters or {},
            "container": container,
        },
    }


def _record(s3: S3Client, store: S3RecordStore, context: JobContext) -> Any:
    body = s3.get_object(Bucket=store.bucket, Key=store.canonical_key(context))["Body"]
    return json.loads(body.read())


def _messages(sqs: SQSClient, queue_url: str) -> list[Any]:
    return sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10).get(
        "Messages", []
    )


class TestSentinelJobs:
    def test_records_each_granule_and_clears_the_claim(
        self, handler: Any, store: S3RecordStore, s3: S3Client
    ) -> None:
        for context in JOB_GROUP.contexts():
            store.write_state_pointer_conditional(context=context, state=SUBMITTED)

        event = _event("RUNNING", parameters=JOB_GROUP.to_batch_parameters())
        assert handler(event, None) == {"state": "AWAITING"}

        for context in JOB_GROUP.contexts():
            assert _record(s3, store, context)["current_state"] == "AWAITING"
            key = store.state_pointer_key(SUBMITTED, context)
            assert not s3.list_objects_v2(Bucket=store.bucket, Prefix=key).get(
                "KeyCount"
            )

    @pytest.mark.parametrize(
        ["exit_code", "state"], [(3, "LOW_SUN_ANGLE"), (4, "CLOUDY")]
    )
    def test_expected_outcomes_are_not_dead_lettered(
        self,
        handler: Any,
        sqs: SQSClient,
        failure_dlq: str,
        exit_code: int,
        state: str,
    ) -> None:
        event = _event(
            "FAILED", exit_code=exit_code, parameters=JOB_GROUP.to_batch_parameters()
        )
        assert handler(event, None) == {"state": state}
        assert _messages(sqs, failure_dlq) == []

    def test_errors_are_dead_lettered(
        self, handler: Any, sqs: SQSClient, failure_dlq: str
    ) -> None:
        event = _event(
            "FAILED", exit_code=1, parameters=JOB_GROUP.to_batch_parameters()
        )
        assert handler(event, None) == {"state": "FAILURE_NONRETRYABLE"}
        assert len(_messages(sqs, failure_dlq)) == 1


class TestPhase0Jobs:
    def _context(self, attempt: int) -> JobContext:
        return JobContext(
            job_type=PHASE0_SENTINEL,
            partition_fields={"acquisition_date": ACQUISITION_DATE},
            input_entity_id=SAFE_ID,
            output_entity_id=GRANULE_ID_STR,
            attempt=attempt,
        )

    def test_records_a_shadow_job(
        self, handler: Any, store: S3RecordStore, s3: S3Client
    ) -> None:
        event = _event("SUCCEEDED", env={"GRANULE_LIST": SAFE_ID})
        assert handler(event, None) == {"state": "SUCCESS"}
        assert _record(s3, store, self._context(1))["batch_job_id"] == "job-1"

    def test_a_resubmitted_shadow_job_is_the_next_attempt(
        self, handler: Any, store: S3RecordStore, s3: S3Client
    ) -> None:
        handler(_event("FAILED", exit_code=1, env={"GRANULE_LIST": SAFE_ID}), None)
        handler(
            _event("SUCCEEDED", job_id="job-2", env={"GRANULE_LIST": SAFE_ID}), None
        )
        assert _record(s3, store, self._context(2))["batch_job_id"] == "job-2"

    def test_failures_are_not_routed(
        self, handler: Any, sqs: SQSClient, failure_dlq: str
    ) -> None:
        event = _event("FAILED", exit_code=1, env={"GRANULE_LIST": SAFE_ID})
        assert handler(event, None) == {"state": "FAILURE_NONRETRYABLE"}
        assert _messages(sqs, failure_dlq) == []

    def test_unrecognized_jobs_are_untracked(self, handler: Any) -> None:
        assert handler(_event("SUCCEEDED", env={}), None) == {"state": "UNTRACKED"}
