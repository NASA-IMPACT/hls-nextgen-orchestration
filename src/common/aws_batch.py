from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypedDict, cast

import boto3

from common.models import (
    EXIT_CODE_CLOUDY,
    EXIT_CODE_LOW_SUN_ANGLE,
    GranuleProcessingEvent,
    ProcessingState,
)

if TYPE_CHECKING:
    from mypy_boto3_batch.client import BatchClient
    from mypy_boto3_batch.literals import JobStatusType
    from mypy_boto3_batch.type_defs import (
        JobDetailTypeDef,
    )


ACTIVE_JOB_STATUSES: set[JobStatusType] = {
    "SUBMITTED",
    "PENDING",
    "RUNNABLE",
    "STARTING",
    "RUNNING",
}

# Phase 0 (existing Step Functions system) env var names
_PHASE0_SENTINEL_KEY = "GRANULE_LIST"
_PHASE0_LANDSAT_AC_KEY = "GRANULE"
_PHASE0_LANDSAT_TILE_MGRS_KEY = "MGRS"
_PHASE0_LANDSAT_TILE_PATHROW_KEY = "PATHROW_LIST"

# Phase 1 (new orchestration) env var names
_PHASE1_WORKFLOW_KEY = "WORKFLOW"

# AWS Batch default when a job definition sets no awslogs-group
_DEFAULT_LOG_GROUP = "/aws/batch/job"


class JobChangeEvent(TypedDict):
    """Type hint for AWS Batch job change events"""

    version: str
    id: str
    detail_type: str
    source: str
    account: str
    time: str
    region: str
    resources: list[str]
    detail: JobDetailTypeDef


def _convert_safe_id_to_hls_id(safe_id: str) -> str:
    """Convert a Sentinel-2 SAFE ID to an HLS S30 granule ID."""
    parts = safe_id.split("_")
    date_str = parts[2][:15]
    year = date_str[0:4]
    month = date_str[4:6]
    day = date_str[6:8]
    hms = date_str[8:15]
    acq_dt = dt.datetime.strptime(f"{year}{month}{day}", "%Y%m%d")
    doy = f"{acq_dt.timetuple().tm_yday:03d}"
    tile = parts[5]
    return f"HLS.S30.{tile}.{year}{doy}{hms}.v2.0"


def _ms_to_iso(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.UTC).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


@dataclass
class JobDetails:
    """Container for accessing properties about an AWS Batch job details"""

    detail: JobDetailTypeDef

    @property
    def job_id(self) -> str:
        return cast(str, self.detail["jobId"])

    @property
    def job_attempts(self) -> int:
        return len(self.detail.get("attempts", []))

    @property
    def max_attempts(self) -> int:
        return cast(int, self.detail.get("retryStrategy", {}).get("attempts", 1))

    @property
    def exit_code(self) -> int | None:
        return cast(int | None, self.detail.get("container", {}).get("exitCode"))

    @property
    def status_reason(self) -> str:
        return cast(str, self.detail.get("statusReason", ""))

    @property
    def log_group_name(self) -> str | None:
        """CloudWatch Logs group the job writes to.

        Returns None for job definitions using a non-awslogs driver, which
        do not ship container output to CloudWatch Logs at all.
        """
        config = cast(
            "dict[str, Any]",
            self.detail.get("container", {}).get("logConfiguration", {}),
        )
        if config and config.get("logDriver") != "awslogs":
            return None
        return cast(
            str, config.get("options", {}).get("awslogs-group", _DEFAULT_LOG_GROUP)
        )

    @property
    def log_stream_name(self) -> str | None:
        """CloudWatch Logs stream for the most recent container attempt."""
        stream = self.detail.get("container", {}).get("logStreamName")
        if stream:
            return cast(str, stream)
        attempts = self.detail.get("attempts", [])
        if attempts:
            return cast(
                "str | None", attempts[-1].get("container", {}).get("logStreamName")
            )
        return None

    @property
    def created_at(self) -> str:
        """Job creation timestamp as ISO-8601 string."""
        return _ms_to_iso(cast(int, self.detail["createdAt"]))

    @property
    def stopped_at(self) -> str:
        """Job stop timestamp as ISO-8601 string."""
        return _ms_to_iso(cast(int, self.detail["stoppedAt"]))

    def _env(self) -> dict[str, str]:
        return {
            entry["name"]: entry["value"]
            for entry in self.detail.get("container", {}).get("environment", [])
        }

    def is_phase1_job(self) -> bool:
        """True if job was submitted by the Phase 1 orchestration (new env vars)."""
        return _PHASE1_WORKFLOW_KEY in self._env()

    def get_job_state(self) -> ProcessingState:
        """Determine ProcessingState from job exit code and status reason."""
        if self.exit_code == 0:
            return ProcessingState.SUCCESS
        if self.exit_code == EXIT_CODE_LOW_SUN_ANGLE:
            return ProcessingState.LOW_SUN_ANGLE
        if self.exit_code == EXIT_CODE_CLOUDY:
            return ProcessingState.CLOUDY
        if self.exit_code is None and self.status_reason.startswith("Host EC2"):
            return ProcessingState.FAILURE_RETRYABLE
        return ProcessingState.FAILURE_NONRETRYABLE

    def get_job_info(self) -> JobDetailTypeDef:
        return self.detail

    def get_granule_event(self) -> GranuleProcessingEvent:
        """Parse Phase 1 GranuleProcessingEvent from new env vars."""
        env = self._env()
        return GranuleProcessingEvent.from_envvar(env)

    def get_shadow_granule_event(self) -> GranuleProcessingEvent:
        """Parse Phase 0 shadow GranuleProcessingEvent from existing system env vars.

        Detects workflow from which Phase 0 env vars are present:
          - GRANULE_LIST  → sentinel
          - GRANULE       → landsat-ac
          - MGRS          → landsat-tile
        """
        env = self._env()
        attempt = int(env.get("ATTEMPT", "0"))

        if _PHASE0_SENTINEL_KEY in env:
            safe_ids = [s for s in env[_PHASE0_SENTINEL_KEY].split(",") if s]
            output_granule_id = (
                _convert_safe_id_to_hls_id(safe_ids[0]) if safe_ids else ""
            )
            # acquisition_date from first SAFE ID date component
            try:
                date_str = safe_ids[0].split("_")[2][:8]
                acq_dt = dt.datetime.strptime(date_str, "%Y%m%d")
                acquisition_date = acq_dt.strftime("%Y-%m-%d")
            except (IndexError, ValueError):
                acquisition_date = dt.datetime.fromtimestamp(
                    cast(int, self.detail["createdAt"]) / 1000, tz=dt.UTC
                ).strftime("%Y-%m-%d")
            return GranuleProcessingEvent(
                workflow="sentinel",
                acquisition_date=acquisition_date,
                source_granule_ids=safe_ids,
                output_granule_id=output_granule_id,
                attempt=attempt,
            )

        if _PHASE0_LANDSAT_AC_KEY in env:
            granule = env[_PHASE0_LANDSAT_AC_KEY]
            acquisition_date = dt.datetime.fromtimestamp(
                cast(int, self.detail["createdAt"]) / 1000, tz=dt.UTC
            ).strftime("%Y-%m-%d")
            return GranuleProcessingEvent(
                workflow="landsat-ac",
                acquisition_date=acquisition_date,
                source_granule_ids=[granule],
                output_granule_id=granule,  # best available for Phase 0 observability
                attempt=attempt,
            )

        if _PHASE0_LANDSAT_TILE_MGRS_KEY in env:
            mgrs = env[_PHASE0_LANDSAT_TILE_MGRS_KEY]
            pathrows = [
                p for p in env.get(_PHASE0_LANDSAT_TILE_PATHROW_KEY, "").split(",") if p
            ]
            acq_dt = dt.datetime.fromtimestamp(
                cast(int, self.detail["createdAt"]) / 1000, tz=dt.UTC
            )
            acquisition_date = acq_dt.strftime("%Y-%m-%d")
            doy = f"{acq_dt.timetuple().tm_yday:03d}"
            output_granule_id = f"HLS.L30.{mgrs}.{acq_dt.year}{doy}T000000.v2.0"
            return GranuleProcessingEvent(
                workflow="landsat-tile",
                acquisition_date=acquisition_date,
                source_granule_ids=pathrows,
                output_granule_id=output_granule_id,
                attempt=attempt,
            )

        # Fallback: unknown workflow, use job ID as placeholder
        acquisition_date = dt.datetime.fromtimestamp(
            cast(int, self.detail["createdAt"]) / 1000, tz=dt.UTC
        ).strftime("%Y-%m-%d")
        return GranuleProcessingEvent(
            workflow="unknown",
            acquisition_date=acquisition_date,
            source_granule_ids=[self.job_id],
            output_granule_id=self.job_id,
            attempt=attempt,
        )


@dataclass
class AwsBatchClient:
    """A high level client for interfacing with AWS Batch"""

    queue: str
    job_definition: str
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

    def submit_job(
        self,
        event: GranuleProcessingEvent,
        output_bucket: str,
    ) -> str:
        """Submit granule processing event to queue, returning job ID."""
        # Use output_granule_id for the job name (safe characters only)
        job_name = event.output_granule_id.replace(".", "-") + f"_{event.attempt}"
        resp = self.client.submit_job(
            jobDefinition=self.job_definition,
            jobName=job_name,
            jobQueue=self.queue,
            containerOverrides={
                "environment": [
                    {"name": "OUTPUT_BUCKET", "value": output_bucket},
                    *event.to_environment(),
                ]
            },
        )
        return cast(str, resp["jobId"])
