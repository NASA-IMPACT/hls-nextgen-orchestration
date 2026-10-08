from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from mypy_boto3_batch.client import BatchClient
    from mypy_boto3_batch.literals import JobStatusType


ACTIVE_JOB_STATUSES: set[JobStatusType] = {
    "SUBMITTED",
    "PENDING",
    "RUNNABLE",
    "STARTING",
    "RUNNING",
}


@dataclass
class AwsBatchClient:
    """A high level client for interfacing with AWS Batch"""

    queue: str
    client: BatchClient = field(default_factory=lambda: boto3.client("batch"))

    def active_jobs_below_threshold(self, threshold: int) -> bool:
        """Return True if the active job count is below the given threshold."""
        paginator = self.client.get_paginator("list_jobs")
        job_count = 0
        for status in ACTIVE_JOB_STATUSES:
            for page in paginator.paginate(
                jobQueue=self.queue,
                jobStatus=status,
            ):
                jobs = page.get("jobSummaryList", [])
                job_count += len(jobs)
                if job_count >= threshold:
                    return False
        return job_count < threshold
