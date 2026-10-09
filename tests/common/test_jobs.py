"""Tests for the shared job types and claim-then-submit."""

from typing import Any
from unittest.mock import MagicMock

import pytest
from batch_event_job_monitor import JobDetails, RetryPolicy, S3RecordStore
from mypy_boto3_s3 import S3Client

from common.jobs import (
    AWAITING_ANCILLARY,
    SUBMITTED,
    build_submit_job_params,
    claim_and_submit,
    exit_code_outcomes,
    sentinel_job_group,
    write_awaiting_ancillary,
)
from tests.conftest import ACQUISITION_DATE, GRANULE_ID_STR, SAFE_ID, TWIN_SAFE_ID

JOB_GROUP = sentinel_job_group(
    acquisition_date=ACQUISITION_DATE,
    source_granule_ids=[SAFE_ID, TWIN_SAFE_ID],
    output_granule_id=GRANULE_ID_STR,
)


def _exists(s3: S3Client, store: S3RecordStore, state: Any, context: Any) -> bool:
    key = store.state_pointer_key(state, context)
    return s3.list_objects_v2(Bucket=store.bucket, Prefix=key).get("KeyCount", 0) == 1


def _submit(store: S3RecordStore) -> str | None:
    return claim_and_submit(
        store=store,
        batch_client=MagicMock(),
        job_group=JOB_GROUP,
        job_queue="queue",
        job_definition="job-definition",
        output_bucket="outputs",
    )


class TestExitCodeOutcomes:
    @pytest.mark.parametrize(
        ["exit_code", "state"], [(3, "LOW_SUN_ANGLE"), (4, "CLOUDY")]
    )
    def test_expected_outcomes_are_not_dead_lettered(
        self, exit_code: int, state: str
    ) -> None:
        job = JobDetails.from_event(
            {"jobId": "j", "status": "FAILED", "container": {"exitCode": exit_code}}
        )
        outcome = job.classify(RetryPolicy(), exit_code_outcomes())
        assert outcome.name == state
        assert not outcome.dlq


class TestBuildSubmitJobParams:
    def test_sets_the_container_environment(self) -> None:
        params = build_submit_job_params(
            JOB_GROUP,
            job_queue="queue",
            job_definition="job-definition",
            output_bucket="outputs",
        )
        assert params["jobQueue"] == "queue"
        assert params["jobDefinition"] == "job-definition"
        env = {
            e["name"]: e["value"] for e in params["containerOverrides"]["environment"]
        }
        assert env == {
            "OUTPUT_BUCKET": "outputs",
            "WORKFLOW": "sentinel",
            "ACQUISITION_DATE": ACQUISITION_DATE,
            "SOURCE_GRANULE_IDS": f"{SAFE_ID},{TWIN_SAFE_ID}",
            "OUTPUT_GRANULE_ID": GRANULE_ID_STR,
            "ATTEMPT": "1",
        }


class TestClaimAndSubmit:
    def test_submits_and_replaces_awaiting_with_submitted(
        self, store: S3RecordStore, s3: S3Client, mocked_submit_job: MagicMock
    ) -> None:
        write_awaiting_ancillary(store, JOB_GROUP)

        assert _submit(store) == "foo-job-id"

        mocked_submit_job.assert_called_once()
        for context in JOB_GROUP.contexts():
            assert _exists(s3, store, SUBMITTED, context)
            assert not _exists(s3, store, AWAITING_ANCILLARY, context)

    def test_a_claimed_granule_is_not_submitted_again(
        self, store: S3RecordStore, s3: S3Client, mocked_submit_job: MagicMock
    ) -> None:
        _, twin = JOB_GROUP.contexts()
        store.write_state_pointer_conditional(context=twin, state=SUBMITTED)

        assert _submit(store) is None

        mocked_submit_job.assert_not_called()
        first, _ = JOB_GROUP.contexts()
        assert not _exists(s3, store, SUBMITTED, first)

    def test_a_failed_submission_releases_its_claims(
        self, store: S3RecordStore, s3: S3Client, mocked_submit_job: MagicMock
    ) -> None:
        write_awaiting_ancillary(store, JOB_GROUP)
        mocked_submit_job.side_effect = RuntimeError("Batch is down")

        with pytest.raises(RuntimeError):
            _submit(store)

        for context in JOB_GROUP.contexts():
            assert not _exists(s3, store, SUBMITTED, context)
            assert _exists(s3, store, AWAITING_ANCILLARY, context)
