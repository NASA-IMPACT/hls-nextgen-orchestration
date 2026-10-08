"""Identify the existing (Phase 0) system's Batch jobs for shadow monitoring.

Phase 0 jobs carry no bejm_* parameters, so their identity is read from the
container environment the existing system sets, and their attempt from the
canonical records already written for the same granules.
"""

from __future__ import annotations

import datetime as dt

from batch_event_job_monitor import JobDetails, JobGroup, S3RecordStore

from common.jobs import (
    ACQUISITION_DATE,
    PHASE0_LANDSAT_AC,
    PHASE0_LANDSAT_TILE,
    PHASE0_SENTINEL,
)
from common.models import convert_safe_id_to_hls_id

# Environment variables that tell the existing system's job types apart
_SENTINEL_KEY = "GRANULE_LIST"
_LANDSAT_AC_KEY = "GRANULE"
_LANDSAT_TILE_MGRS_KEY = "MGRS"
_LANDSAT_TILE_PATHROW_KEY = "PATHROW_LIST"


def _created_date(job: JobDetails) -> dt.datetime:
    created_at = job.created_at
    if created_at is None:
        raise ValueError(f"Batch job {job.job_id!r} has no createdAt")
    return created_at


def _sentinel(env: dict[str, str], job: JobDetails) -> JobGroup | None:
    safe_ids = [s for s in env[_SENTINEL_KEY].split(",") if s]
    if not safe_ids:
        return None
    try:
        acquisition_date = dt.datetime.strptime(
            safe_ids[0].split("_")[2][:8], "%Y%m%d"
        ).strftime("%Y-%m-%d")
    except (IndexError, ValueError):
        acquisition_date = _created_date(job).strftime("%Y-%m-%d")
    return JobGroup(
        job_type=PHASE0_SENTINEL,
        partition_fields={ACQUISITION_DATE: acquisition_date},
        input_entity_ids=safe_ids,
        output_entity_id=convert_safe_id_to_hls_id(safe_ids[0]),
        attempt=1,
    )


def _landsat_ac(env: dict[str, str], job: JobDetails) -> JobGroup:
    granule = env[_LANDSAT_AC_KEY]
    return JobGroup(
        job_type=PHASE0_LANDSAT_AC,
        partition_fields={ACQUISITION_DATE: _created_date(job).strftime("%Y-%m-%d")},
        input_entity_ids=[granule],
        # Phase 0 sets no output granule ID; the input is the closest stand-in
        output_entity_id=granule,
        attempt=1,
    )


def _landsat_tile(env: dict[str, str], job: JobDetails) -> JobGroup | None:
    mgrs = env[_LANDSAT_TILE_MGRS_KEY]
    pathrows = [p for p in env.get(_LANDSAT_TILE_PATHROW_KEY, "").split(",") if p]
    if not pathrows:
        return None
    created = _created_date(job)
    doy = f"{created.timetuple().tm_yday:03d}"
    return JobGroup(
        job_type=PHASE0_LANDSAT_TILE,
        partition_fields={ACQUISITION_DATE: created.strftime("%Y-%m-%d")},
        input_entity_ids=pathrows,
        output_entity_id=f"HLS.L30.{mgrs}.{created.year}{doy}T000000.v2.0",
        attempt=1,
    )


def phase0_job_group(job: JobDetails) -> JobGroup | None:
    """The JobGroup of a Phase 0 job at attempt 1, or None if unrecognized."""
    env = job.environment
    if _SENTINEL_KEY in env:
        return _sentinel(env, job)
    if _LANDSAT_AC_KEY in env:
        return _landsat_ac(env, job)
    if _LANDSAT_TILE_MGRS_KEY in env:
        return _landsat_tile(env, job)
    return None


def resolve_phase0_job(job: JobDetails, log_store: S3RecordStore) -> JobGroup | None:
    """Untracked-job resolver for the job monitor's Phase 0 job types.

    The existing system resubmits each retry as a new Batch job without
    recording which attempt it is, so the attempt is inferred from the
    granules' canonical records.
    """
    job_group = phase0_job_group(job)
    if job_group is None:
        return None
    attempt = log_store.attempt_for_batch_job(
        job_type=job_group.job_type,
        partition_fields=job_group.partition_fields,
        input_entity_ids=job_group.input_entity_ids,
        batch_job_id=job.job_id,
    )
    return JobGroup(
        job_type=job_group.job_type,
        partition_fields=job_group.partition_fields,
        input_entity_ids=job_group.input_entity_ids,
        output_entity_id=job_group.output_entity_id,
        attempt=attempt,
    )
