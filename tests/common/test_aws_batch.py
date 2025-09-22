import datetime as dt
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Iterator
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from mypy_boto3_batch import BatchClient
from mypy_boto3_batch.type_defs import JobDetailTypeDef
from pytest_lazy_fixtures import lf

from common import GranuleProcessingEvent, JobOutcome
from common.aws_batch import AwsBatchClient, JobDetails


class TestJobDetail:
    """Tests for JobDetail"""

    @pytest.mark.parametrize(
        ["detail", "attempts"],
        [
            (lf("job_detail_failed_error"), 1),
            (lf("job_detail_failed_spot"), 3),
        ],
    )
    def test_attempts(self, detail: JobDetailTypeDef, attempts: int) -> None:
        job_detail = JobDetails(detail)
        assert job_detail.job_attempts == attempts
        assert job_detail.max_attempts == 3

    @pytest.mark.parametrize(
        ["detail", "exit_code"],
        [
            (lf("job_detail_failed_error"), 1),
            (lf("job_detail_failed_spot"), None),
        ],
    )
    def test_exit_code(self, detail: JobDetailTypeDef, exit_code: int) -> None:
        job_detail = JobDetails(detail)
        assert job_detail.exit_code == exit_code

    def test_get_job_outcome(self, job_detail_failed_error: JobDetailTypeDef) -> None:
        """Test we correctly parse the job outcome"""
        detail = job_detail_failed_error.copy()
        detail["container"]["exitCode"] = 1
        outcome = JobDetails(detail).get_job_outcome()
        assert outcome == JobOutcome.FAILURE_NONRETRYABLE

        detail = job_detail_failed_error.copy()
        detail["container"]["exitCode"] = 0
        outcome = JobDetails(detail).get_job_outcome()
        assert outcome == JobOutcome.SUCCESS

        # SPOT interruption
        detail = deepcopy(job_detail_failed_error)
        del detail["container"]["exitCode"]
        detail["statusReason"] = "Host EC2 (instance i-01225b700440f4809) terminated."
        outcome = JobDetails(detail).get_job_outcome()
        assert outcome == JobOutcome.FAILURE_RETRYABLE

        # Cancelled job
        detail = job_detail_failed_error.copy()
        del detail["container"]["exitCode"]
        detail["statusReason"] = "Manually cancelling this job"
        outcome = JobDetails(detail).get_job_outcome()
        assert outcome == JobOutcome.FAILURE_NONRETRYABLE

    def test_get_granule_event_details(
        self, job_detail_failed_error: JobDetailTypeDef
    ) -> None:
        """Test we correctly parse the job granule processing event details"""
        detail = job_detail_failed_error.copy()
        detail["container"]["environment"] = [
            {"name": "GRANULE_ID", "value": "foo"},
            {"name": "ATTEMPT", "value": "0"},
        ]
        event_details = JobDetails(detail).get_granule_event()

        assert event_details == GranuleProcessingEvent(granule_id="foo", attempt=0)


def make_job_summary_list(count: int, status: str) -> list[dict[str, Any]]:
    """Return an example `list-jobs` response for some status"""
    jobs = []
    for _ in range(count):
        job_id = str(uuid4())
        job_info = {
            "jobArn": f"arn:aws:batch:us-west-2:123456789012:job/{job_id}",
            "jobId": job_id,
            "jobName": "BatchJobNotification",
            "createdAt": (dt.datetime.now() - dt.timedelta(hours=1)).timestamp(),
            "status": status,
            "container": {},
        }

        if status == "RUNNING":
            job_info["startedAt"] = (
                dt.datetime.now() - dt.timedelta(minutes=15)
            ).timestamp()

        jobs.append(job_info)

    return [{"jobSummaryList": jobs}]


@dataclass
class MockListJobsPaginator:
    """Mock paginating through AWS Batch ListJobs"""

    count_by_status: dict[str, int]

    def paginate(self, *, jobStatus: str, **kwds: Any) -> Iterator[dict[str, Any]]:
        """Yield pages of ListJobs responses"""
        count = self.count_by_status.get(jobStatus, 0)
        yield from make_job_summary_list(count=count, status=jobStatus)


class TestAwsBatchClient:
    """Tests for AwsBatchClient"""

    @pytest.fixture
    def client(self, batch: BatchClient) -> AwsBatchClient:
        """A basic fake AwsBatchClient"""
        return AwsBatchClient(
            queue="batch-queue", job_definition="job-definition", client=batch
        )

    def make_mock_list_jobs_paginator(
        self, client: AwsBatchClient, count_by_status: dict[str, int]
    ) -> Iterator[MagicMock]:
        """Create a fake ListJobs paginator"""
        with patch.object(
            client.client,
            "get_paginator",
            return_value=MockListJobsPaginator(count_by_status),
        ) as mocked_get_paginator:
            yield mocked_get_paginator

    def test_active_jobs_below_threshold(self, client: AwsBatchClient) -> None:
        """Test checking if active jobs are below threshold"""
        with patch.object(
            client.client,
            "get_paginator",
            return_value=MockListJobsPaginator({"SUBMITTED": 10, "RUNNING": 5}),
        ) as mocked_get_paginator:
            assert client.active_jobs_below_threshold(200)
        mocked_get_paginator.assert_called()

        with patch.object(
            client.client,
            "get_paginator",
            return_value=MockListJobsPaginator({"SUBMITTED": 10, "RUNNING": 5}),
        ) as mocked_get_paginator:
            assert not client.active_jobs_below_threshold(5)
        mocked_get_paginator.assert_called()
