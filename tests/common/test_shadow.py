"""Tests for identifying Phase 0 jobs."""

from typing import Any

import pytest
from batch_event_job_monitor import JobDetails, S3RecordStore
from batch_event_job_monitor.models import JobContext, ProcessingEventRecord

from common.jobs import PHASE0_LANDSAT_AC, PHASE0_LANDSAT_TILE, PHASE0_SENTINEL
from common.shadow import phase0_job_group, resolve_phase0_job
from tests.conftest import ACQUISITION_DATE, GRANULE_ID_STR, SAFE_ID, TWIN_SAFE_ID

# 2025-04-23T18:57:00Z
CREATED_AT = 1745434620000


def _job(env: dict[str, str], job_id: str = "job-1") -> JobDetails:
    detail: dict[str, Any] = {
        "jobId": job_id,
        "status": "SUCCEEDED",
        "createdAt": CREATED_AT,
        "container": {"environment": [{"name": k, "value": v} for k, v in env.items()]},
    }
    return JobDetails.from_event(detail)


class TestPhase0JobGroup:
    def test_sentinel_twins(self) -> None:
        job_group = phase0_job_group(
            _job({"GRANULE_LIST": f"{SAFE_ID},{TWIN_SAFE_ID}"})
        )
        assert job_group is not None
        assert job_group.job_type == PHASE0_SENTINEL
        assert job_group.partition_fields == {"acquisition_date": ACQUISITION_DATE}
        assert job_group.input_entity_ids == [SAFE_ID, TWIN_SAFE_ID]
        assert job_group.output_entity_id == GRANULE_ID_STR

    def test_landsat_ac(self) -> None:
        granule = "LC08_L1TP_027039_20250423_20250423_02_RT"
        job_group = phase0_job_group(_job({"GRANULE": granule}))
        assert job_group is not None
        assert job_group.job_type == PHASE0_LANDSAT_AC
        assert job_group.partition_fields == {"acquisition_date": "2025-04-23"}
        assert job_group.input_entity_ids == [granule]
        assert job_group.output_entity_id == granule

    def test_landsat_tile(self) -> None:
        job_group = phase0_job_group(
            _job({"MGRS": "T14TPN", "PATHROW_LIST": "027039,027040"})
        )
        assert job_group is not None
        assert job_group.job_type == PHASE0_LANDSAT_TILE
        assert job_group.input_entity_ids == ["027039", "027040"]
        assert job_group.output_entity_id == "HLS.L30.T14TPN.2025113T000000.v2.0"

    @pytest.mark.parametrize(
        "env",
        [{}, {"GRANULE_LIST": ""}, {"MGRS": "T14TPN", "PATHROW_LIST": ""}],
    )
    def test_unrecognized_jobs_resolve_to_none(self, env: dict[str, str]) -> None:
        assert phase0_job_group(_job(env)) is None


class TestResolvePhase0Job:
    def _record(self, store: S3RecordStore, *, attempt: int, batch_job_id: str) -> None:
        store.append_canonical_event(
            context=JobContext(
                job_type=PHASE0_SENTINEL,
                partition_fields={"acquisition_date": ACQUISITION_DATE},
                input_entity_id=SAFE_ID,
                output_entity_id=GRANULE_ID_STR,
                attempt=attempt,
            ),
            event=ProcessingEventRecord(state="FAILURE_NONRETRYABLE", timestamp="t"),
            batch_job_id=batch_job_id,
        )

    def test_first_job_is_attempt_one(self, store: S3RecordStore) -> None:
        job_group = resolve_phase0_job(_job({"GRANULE_LIST": SAFE_ID}), store)
        assert job_group is not None
        assert job_group.attempt == 1

    def test_a_resubmission_is_the_next_attempt(self, store: S3RecordStore) -> None:
        self._record(store, attempt=1, batch_job_id="job-1")
        job_group = resolve_phase0_job(_job({"GRANULE_LIST": SAFE_ID}, "job-2"), store)
        assert job_group is not None
        assert job_group.attempt == 2

    def test_unrecognized_jobs_are_left_untracked(self, store: S3RecordStore) -> None:
        assert resolve_phase0_job(_job({}), store) is None
