import datetime as dt
from collections.abc import Iterator
from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import pytest
from mypy_boto3_batch import BatchClient
from mypy_boto3_batch.type_defs import JobDetailTypeDef
from pytest_lazy_fixtures import lf

from common import ProcessingState
from common.aws_batch import AwsBatchClient, JobDetails
from tests.conftest import (
    ACQUISITION_DATE,
    GRANULE_ID_STR,
    LOG_GROUP_NAME,
    LOG_STREAM_NAME,
    SAFE_ID,
)


class TestJobDetails:
    @pytest.mark.parametrize(
        ["detail", "attempts"],
        [
            (lf("job_detail_failed_error"), 1),
            (lf("job_detail_failed_spot"), 3),
        ],
    )
    def test_attempts(self, detail: JobDetailTypeDef, attempts: int) -> None:
        jd = JobDetails(detail)
        assert jd.job_attempts == attempts
        assert jd.max_attempts == 3

    @pytest.mark.parametrize(
        ["detail", "exit_code"],
        [
            (lf("job_detail_failed_error"), 1),
            (lf("job_detail_failed_spot"), None),
        ],
    )
    def test_exit_code(self, detail: JobDetailTypeDef, exit_code: int | None) -> None:
        assert JobDetails(detail).exit_code == exit_code

    @pytest.mark.parametrize(
        "detail",
        [lf("job_detail_failed_error"), lf("job_detail_failed_spot")],
    )
    def test_log_stream_name(self, detail: JobDetailTypeDef) -> None:
        assert JobDetails(detail).log_stream_name == LOG_STREAM_NAME

    def test_log_group_name(self, job_detail_failed_error: JobDetailTypeDef) -> None:
        assert JobDetails(job_detail_failed_error).log_group_name == LOG_GROUP_NAME

    def test_log_group_name_defaults_when_unset(
        self, job_detail_failed_error: JobDetailTypeDef
    ) -> None:
        detail = deepcopy(job_detail_failed_error)
        del detail["container"]["logConfiguration"]
        assert JobDetails(detail).log_group_name == "/aws/batch/job"

    def test_log_group_name_none_for_non_awslogs_driver(
        self, job_detail_failed_error: JobDetailTypeDef
    ) -> None:
        detail = deepcopy(job_detail_failed_error)
        detail["container"]["logConfiguration"] = {"logDriver": "splunk", "options": {}}
        assert JobDetails(detail).log_group_name is None

    def test_log_stream_name_falls_back_to_attempts(
        self, job_detail_failed_spot: JobDetailTypeDef
    ) -> None:
        detail = deepcopy(job_detail_failed_spot)
        del detail["container"]["logStreamName"]
        assert JobDetails(detail).log_stream_name == LOG_STREAM_NAME

    def test_log_stream_name_missing(
        self, job_detail_failed_error: JobDetailTypeDef
    ) -> None:
        detail = deepcopy(job_detail_failed_error)
        del detail["container"]["logStreamName"]
        detail["attempts"] = []
        assert JobDetails(detail).log_stream_name is None

    def test_get_job_state_success(self, job_detail_success: JobDetailTypeDef) -> None:
        assert JobDetails(job_detail_success).get_job_state() == ProcessingState.SUCCESS

    def test_get_job_state_cloudy(self, job_detail_cloudy: JobDetailTypeDef) -> None:
        assert JobDetails(job_detail_cloudy).get_job_state() == ProcessingState.CLOUDY

    def test_get_job_state_low_sun(self, job_detail_low_sun: JobDetailTypeDef) -> None:
        state = JobDetails(job_detail_low_sun).get_job_state()
        assert state == ProcessingState.LOW_SUN_ANGLE

    def test_get_job_state_nonretryable(
        self, job_detail_failed_error: JobDetailTypeDef
    ) -> None:
        assert (
            JobDetails(job_detail_failed_error).get_job_state()
            == ProcessingState.FAILURE_NONRETRYABLE
        )

    def test_get_job_state_retryable_spot(
        self, job_detail_failed_spot: JobDetailTypeDef
    ) -> None:
        assert (
            JobDetails(job_detail_failed_spot).get_job_state()
            == ProcessingState.FAILURE_RETRYABLE
        )

    def test_get_job_state_cancelled(
        self, job_detail_failed_error: JobDetailTypeDef
    ) -> None:
        detail = deepcopy(job_detail_failed_error)
        del detail["container"]["exitCode"]
        detail["statusReason"] = "Manually cancelled"
        state = JobDetails(detail).get_job_state()
        assert state == ProcessingState.FAILURE_NONRETRYABLE

    def test_is_phase1_job(self, job_detail_success: JobDetailTypeDef) -> None:
        assert JobDetails(job_detail_success).is_phase1_job()

    def test_is_not_phase1_job_shadow(self) -> None:
        from tests.conftest import _make_job_detail

        detail = _make_job_detail(
            exit_code=0,
            env=[
                {"name": "GRANULE_LIST", "value": SAFE_ID},
                {"name": "ATTEMPT", "value": "0"},
            ],
        )
        assert not JobDetails(detail).is_phase1_job()

    def test_get_granule_event_phase1(
        self, job_detail_success: JobDetailTypeDef
    ) -> None:
        event = JobDetails(job_detail_success).get_granule_event()
        assert event.workflow == "sentinel"
        assert event.acquisition_date == ACQUISITION_DATE
        assert event.source_granule_ids == [SAFE_ID]
        assert event.output_granule_id == GRANULE_ID_STR
        assert event.attempt == 0

    def test_get_shadow_granule_event_sentinel(self) -> None:
        from tests.conftest import _make_job_detail

        detail = _make_job_detail(
            exit_code=0,
            env=[
                {"name": "GRANULE_LIST", "value": SAFE_ID},
                {"name": "ATTEMPT", "value": "0"},
            ],
        )
        event = JobDetails(detail).get_shadow_granule_event()
        assert event.workflow == "sentinel"
        assert event.source_granule_ids == [SAFE_ID]
        assert event.output_granule_id == GRANULE_ID_STR

    def test_get_shadow_granule_event_landsat_ac(self) -> None:
        from tests.conftest import _make_job_detail

        detail = _make_job_detail(
            exit_code=0,
            env=[
                {"name": "GRANULE", "value": "LC08_L1TP_043033_20210601"},
                {"name": "ATTEMPT", "value": "0"},
            ],
        )
        event = JobDetails(detail).get_shadow_granule_event()
        assert event.workflow == "landsat-ac"
        assert event.source_granule_ids == ["LC08_L1TP_043033_20210601"]

    def test_get_shadow_granule_event_landsat_tile(self) -> None:
        from tests.conftest import _make_job_detail

        detail = _make_job_detail(
            exit_code=0,
            env=[
                {"name": "MGRS", "value": "T18TYN"},
                {"name": "PATHROW_LIST", "value": "043033,043034"},
                {"name": "ATTEMPT", "value": "0"},
            ],
        )
        event = JobDetails(detail).get_shadow_granule_event()
        assert event.workflow == "landsat-tile"
        assert "T18TYN" in event.output_granule_id
        assert len(event.source_granule_ids) == 2

    def test_created_at_stopped_at(self, job_detail_success: JobDetailTypeDef) -> None:
        jd = JobDetails(job_detail_success)
        # Just assert they return ISO strings
        assert "T" in jd.created_at
        assert "T" in jd.stopped_at


def make_job_summary_list(count: int, status: str) -> list[dict[str, Any]]:
    jobs = []
    for _ in range(count):
        job_id = str(uuid4())
        job_info: dict[str, Any] = {
            "jobArn": f"arn:aws:batch:us-west-2:123456789012:job/{job_id}",
            "jobId": job_id,
            "jobName": "test-job",
            "createdAt": (dt.datetime.now() - dt.timedelta(hours=1)).timestamp(),
            "status": status,
            "container": {},
        }
        jobs.append(job_info)
    return [{"jobSummaryList": jobs}]


@dataclass
class MockListJobsPaginator:
    count_by_status: dict[str, int]

    def paginate(self, *, jobStatus: str, **kwds: Any) -> Iterator[dict[str, Any]]:
        count = self.count_by_status.get(jobStatus, 0)
        yield from make_job_summary_list(count=count, status=jobStatus)


class TestAwsBatchClient:
    @pytest.fixture
    def client(self, batch: BatchClient) -> AwsBatchClient:
        return AwsBatchClient(
            queue="batch-queue", job_definition="job-definition", client=batch
        )

    def test_active_jobs_below_threshold_true(self, client: AwsBatchClient) -> None:
        with patch.object(
            client.client,
            "get_paginator",
            return_value=MockListJobsPaginator({"SUBMITTED": 10, "RUNNING": 5}),
        ):
            assert client.active_jobs_below_threshold(200)

    def test_active_jobs_below_threshold_false(self, client: AwsBatchClient) -> None:
        with patch.object(
            client.client,
            "get_paginator",
            return_value=MockListJobsPaginator({"SUBMITTED": 10, "RUNNING": 5}),
        ):
            assert not client.active_jobs_below_threshold(5)
