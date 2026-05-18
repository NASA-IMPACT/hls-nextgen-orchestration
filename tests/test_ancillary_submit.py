"""Tests for the ancillary_submit Lambda (per-granule Batch submission)."""

import json
from unittest.mock import patch

import boto3
import pytest

from ancillary_submit.handler import process_granule
from common import ProcessingEventRecord, ProcessingState, S3RecordStore
from tests.conftest import ACQUISITION_DATE, GRANULE_ID_STR, SAFE_ID

_BATCH_QUEUE = "test-queue"
_JOB_DEF = "test-jd"


@pytest.fixture
def store(bucket: str) -> S3RecordStore:
    return S3RecordStore(bucket=bucket)


@pytest.fixture
def awaiting_granule(store: S3RecordStore) -> None:
    """Pre-populate an AWAITING canonical record and state pointer."""
    store.append_canonical_event(
        source_granule_id=SAFE_ID,
        output_granule_id=GRANULE_ID_STR,
        workflow="sentinel",
        acquisition_date=ACQUISITION_DATE,
        attempt=0,
        event=ProcessingEventRecord(state="AWAITING", ts="2023-08-17T00:00:00Z"),
        shadow=False,
    )
    store.write_state_pointer(
        workflow="sentinel",
        acquisition_date=ACQUISITION_DATE,
        source_granule_id=SAFE_ID,
        attempt=0,
        new_state=ProcessingState.AWAITING,
        old_state=None,
        output_granule_id=GRANULE_ID_STR,
    )


def _run(store: S3RecordStore, aux_bucket: str, output_bucket: str) -> bool:
    return process_granule(
        source_granule_id=SAFE_ID,
        output_granule_id=GRANULE_ID_STR,
        attempt=0,
        acquisition_date=ACQUISITION_DATE,
        aux_bucket=aux_bucket,
        processing_bucket=store.bucket,
        batch_queue=_BATCH_QUEUE,
        job_definition=_JOB_DEF,
        output_bucket=output_bucket,
    )


class TestProcessGranule:
    def test_granule_submitted(
        self,
        store: S3RecordStore,
        aux_bucket: str,
        output_bucket: str,
        awaiting_granule: None,
        mocked_batch_client_submit_job: object,
    ) -> None:
        result = _run(store, aux_bucket, output_bucket)
        assert result is True

        s3 = boto3.client("s3", region_name="us-west-2")

        # AWAITING pointer removed
        awaiting_key = S3RecordStore.state_pointer_key(
            ProcessingState.AWAITING, "sentinel", ACQUISITION_DATE, SAFE_ID, 0
        )
        assert (
            s3.list_objects_v2(Bucket=store.bucket, Prefix=awaiting_key).get(
                "KeyCount", 0
            )
            == 0
        )

        # SUBMITTED pointer written
        submitted_key = S3RecordStore.state_pointer_key(
            ProcessingState.SUBMITTED, "sentinel", ACQUISITION_DATE, SAFE_ID, 0
        )
        assert (
            s3.list_objects_v2(Bucket=store.bucket, Prefix=submitted_key).get(
                "KeyCount", 0
            )
            == 1
        )

        # Canonical record has SUBMITTED event appended
        key = S3RecordStore.canonical_key("sentinel", ACQUISITION_DATE, SAFE_ID, 0)
        record = json.loads(s3.get_object(Bucket=store.bucket, Key=key)["Body"].read())
        assert record["current_state"] == "SUBMITTED"
        assert any(e["state"] == "SUBMITTED" for e in record["events"])

    def test_aux_not_available_skips_granule(
        self,
        store: S3RecordStore,
        output_bucket: str,
        awaiting_granule: None,
        mocked_batch_client_submit_job: object,
    ) -> None:
        s3 = boto3.client("s3", region_name="us-west-2")
        empty_aux = "empty-aux-bucket"
        s3.create_bucket(
            Bucket=empty_aux,
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
        result = _run(store, empty_aux, output_bucket)
        assert result is False

    def test_conditional_write_prevents_double_submit(
        self,
        store: S3RecordStore,
        aux_bucket: str,
        output_bucket: str,
        awaiting_granule: None,
        mocked_batch_client_submit_job: object,
    ) -> None:
        with patch.object(
            S3RecordStore, "write_state_pointer_conditional", return_value=False
        ):
            result = _run(store, aux_bucket, output_bucket)
        assert result is False
