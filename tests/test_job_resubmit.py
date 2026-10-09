"""Tests for the job resubmit Lambda."""

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from batch_event_job_monitor import RetryMessage

from common.jobs import SENTINEL, sentinel_job_group, sentinel_job_type_config
from tests.conftest import ACQUISITION_DATE, GRANULE_ID_STR, SAFE_ID

_QUEUE = "arn:aws:batch:us-west-2:123456789012:job-queue/hls-orch-dev"
_JOB_DEFINITION = "arn:aws:batch:us-west-2:123456789012:job-definition/sentinel-ac"


@pytest.fixture
def handler(aws_credentials: None, monkeypatch: pytest.MonkeyPatch) -> Any:
    config = sentinel_job_type_config(
        job_queue_arn=_QUEUE, job_definition_arn=_JOB_DEFINITION, max_attempts=3
    )
    monkeypatch.setenv(
        "PROCESSING_JOB_TYPE_CONFIGS", json.dumps({SENTINEL: config.to_dict()})
    )
    monkeypatch.setenv("OUTPUT_BUCKET_NAME", "outputs")
    from job_resubmit.handler import handler

    return handler


def _sqs_event(body: str) -> dict[str, Any]:
    return {"Records": [{"messageId": "m-1", "body": body}]}


def test_resubmits_the_next_attempt_with_the_container_environment(
    handler: Any,
) -> None:
    job_group = sentinel_job_group(
        acquisition_date=ACQUISITION_DATE,
        source_granule_ids=[SAFE_ID],
        output_granule_id=GRANULE_ID_STR,
    )
    message = RetryMessage.from_job_group(
        job_group, batch_job_id="job-1", state="FAILURE_RETRYABLE"
    )

    with patch("job_resubmit.handler._batch_client") as batch_client:
        batch_client.submit_job.return_value = {"jobId": "job-2"}
        result = handler(_sqs_event(message.to_json()), None)

    assert result == {"batchItemFailures": []}
    params = batch_client.submit_job.call_args.kwargs
    assert params["jobQueue"] == _QUEUE
    assert params["jobDefinition"] == _JOB_DEFINITION
    assert params["parameters"]["bejm_attempt"] == "2"
    env = {e["name"]: e["value"] for e in params["containerOverrides"]["environment"]}
    assert env["ATTEMPT"] == "2"
    assert env["OUTPUT_BUCKET"] == "outputs"


def test_a_malformed_message_is_reported_as_a_failure(handler: Any) -> None:
    with patch("job_resubmit.handler._batch_client", MagicMock()):
        result = handler(_sqs_event("not json"), None)
    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
