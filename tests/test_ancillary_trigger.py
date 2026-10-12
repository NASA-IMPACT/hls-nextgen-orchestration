"""Tests for the ancillary_trigger Lambda (fan-out)."""

import json

import boto3
import pytest
from mypy_boto3_sqs import SQSClient

from ancillary_trigger.handler import process_aux_event
from common import ProcessingEventRecord, ProcessingState, S3RecordStore
from tests.conftest import ACQUISITION_DATE, GRANULE_ID_STR, SAFE_ID

# Matches ACQUISITION_DATE (2023-08-17 = DOY 229)
_AUX_KEY = "lasrc_aux/LADS/2023/VJ104ANC.A2023229"


@pytest.fixture
def store(bucket: str) -> S3RecordStore:
    return S3RecordStore(bucket=bucket)


@pytest.fixture
def submit_queue(sqs: SQSClient) -> str:
    return sqs.create_queue(QueueName="test-ancillary-submit")["QueueUrl"]


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


def _run(store: S3RecordStore, submit_queue_url: str, s3_key: str = _AUX_KEY) -> int:
    return process_aux_event(
        s3_key=s3_key,
        processing_bucket=store.bucket,
        submit_queue_url=submit_queue_url,
    )


class TestProcessAuxEvent:
    def test_unrecognised_key_is_noop(
        self,
        store: S3RecordStore,
        submit_queue: str,
    ) -> None:
        result = _run(store, submit_queue, s3_key="some/other/file.tif")
        assert result == 0

    def test_no_awaiting_granules_is_noop(
        self,
        store: S3RecordStore,
        submit_queue: str,
    ) -> None:
        result = _run(store, submit_queue)
        assert result == 0

    def test_single_awaiting_granule_enqueued(
        self,
        store: S3RecordStore,
        submit_queue: str,
        awaiting_granule: None,
    ) -> None:
        result = _run(store, submit_queue)
        assert result == 1

        sqs = boto3.client("sqs", region_name="us-west-2")
        resp = sqs.receive_message(QueueUrl=submit_queue, MaxNumberOfMessages=10)
        messages = resp.get("Messages", [])
        assert len(messages) == 1

        body = json.loads(messages[0]["Body"])
        assert body["source_granule_id"] == SAFE_ID
        assert body["output_granule_id"] == GRANULE_ID_STR
        assert body["attempt"] == 0
        assert body["acquisition_date"] == ACQUISITION_DATE
