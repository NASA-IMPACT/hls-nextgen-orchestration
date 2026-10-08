import datetime as dt
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import pytest
from mypy_boto3_batch import BatchClient

from common.aws_batch import AwsBatchClient


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
        return AwsBatchClient(queue="batch-queue", client=batch)

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
