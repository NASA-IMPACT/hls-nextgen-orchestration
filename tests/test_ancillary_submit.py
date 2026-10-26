"""Tests for the ancillary_submit Lambda (per-granule Batch submission)."""

from unittest.mock import MagicMock

import boto3
import pytest
from batch_event_job_monitor import S3RecordStore

from ancillary_submit.handler import process_granule
from common.ancillary import LaadsSource
from common.jobs import (
    AWAITING_ANCILLARY,
    SUBMITTED,
    sentinel_job_group,
    write_awaiting_ancillary,
)
from tests.conftest import ACQUISITION_DATE, GRANULE_ID_STR, SAFE_ID, TWIN_SAFE_ID

JOB_GROUP = sentinel_job_group(
    acquisition_date=ACQUISITION_DATE,
    source_granule_ids=[SAFE_ID, TWIN_SAFE_ID],
    output_granule_id=GRANULE_ID_STR,
)


@pytest.fixture
def awaiting_granule(store: S3RecordStore) -> None:
    write_awaiting_ancillary(store, JOB_GROUP)


def _exists(store: S3RecordStore, state: object, context: object) -> bool:
    key = store.state_pointer_key(state, context)  # type: ignore[arg-type]
    s3 = boto3.client("s3", region_name="us-west-2")
    return bool(s3.list_objects_v2(Bucket=store.bucket, Prefix=key).get("KeyCount"))


def _run(aux_bucket: str, output_bucket: str) -> bool:
    return process_granule(
        acquisition_date=ACQUISITION_DATE,
        source_granule_ids=[SAFE_ID, TWIN_SAFE_ID],
        output_granule_id=GRANULE_ID_STR,
        attempt=1,
        ancillary=LaadsSource(aux_bucket),
        batch_queue="test-queue",
        job_definition="test-jd",
        output_bucket=output_bucket,
    )


class TestProcessGranule:
    def test_granule_submitted(
        self,
        store: S3RecordStore,
        aux_bucket: str,
        output_bucket: str,
        awaiting_granule: None,
        mocked_submit_job: MagicMock,
    ) -> None:
        assert _run(aux_bucket, output_bucket) is True

        mocked_submit_job.assert_called_once()
        assert mocked_submit_job.call_args.kwargs["job_group"] == JOB_GROUP
        for context in JOB_GROUP.contexts():
            assert not _exists(store, AWAITING_ANCILLARY, context)
            assert _exists(store, SUBMITTED, context)

    def test_aux_not_available_skips_granule(
        self,
        store: S3RecordStore,
        output_bucket: str,
        awaiting_granule: None,
        mocked_submit_job: MagicMock,
    ) -> None:
        s3 = boto3.client("s3", region_name="us-west-2")
        s3.create_bucket(
            Bucket="empty-aux-bucket",
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
        assert _run("empty-aux-bucket", output_bucket) is False
        mocked_submit_job.assert_not_called()

    def test_a_second_delivery_does_not_submit_twice(
        self,
        store: S3RecordStore,
        aux_bucket: str,
        output_bucket: str,
        awaiting_granule: None,
        mocked_submit_job: MagicMock,
    ) -> None:
        assert _run(aux_bucket, output_bucket) is True
        assert _run(aux_bucket, output_bucket) is False
        mocked_submit_job.assert_called_once()
