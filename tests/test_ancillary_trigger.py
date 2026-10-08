"""Tests for the ancillary_trigger Lambda (fan-out)."""

import json

import boto3
import pytest
from batch_event_job_monitor import S3RecordStore
from mypy_boto3_sqs import SQSClient

from ancillary_trigger.handler import process_aux_event
from common.jobs import sentinel_job_group, write_awaiting_ancillary
from tests.conftest import ACQUISITION_DATE, GRANULE_ID_STR, SAFE_ID, TWIN_SAFE_ID

# Matches ACQUISITION_DATE (2023-08-17 = DOY 229)
_AUX_KEY = "lasrc_aux/LADS/2023/VJ104ANC.A2023229"


@pytest.fixture
def submit_queue(sqs: SQSClient) -> str:
    return sqs.create_queue(QueueName="test-ancillary-submit")["QueueUrl"]


def _await(store: S3RecordStore, source_granule_ids: list[str]) -> None:
    write_awaiting_ancillary(
        store,
        sentinel_job_group(
            acquisition_date=ACQUISITION_DATE,
            source_granule_ids=source_granule_ids,
            output_granule_id=GRANULE_ID_STR,
        ),
    )


def _messages(submit_queue: str) -> list[dict]:
    sqs = boto3.client("sqs", region_name="us-west-2")
    resp = sqs.receive_message(QueueUrl=submit_queue, MaxNumberOfMessages=10)
    return [json.loads(m["Body"]) for m in resp.get("Messages", [])]


class TestProcessAuxEvent:
    def test_unrecognised_key_is_noop(
        self, store: S3RecordStore, submit_queue: str
    ) -> None:
        result = process_aux_event(
            s3_key="some/other/file.tif", submit_queue_url=submit_queue
        )
        assert result == 0

    def test_no_awaiting_granules_is_noop(
        self, store: S3RecordStore, submit_queue: str
    ) -> None:
        assert process_aux_event(s3_key=_AUX_KEY, submit_queue_url=submit_queue) == 0

    def test_single_awaiting_granule_enqueued(
        self, store: S3RecordStore, submit_queue: str
    ) -> None:
        _await(store, [SAFE_ID])

        assert process_aux_event(s3_key=_AUX_KEY, submit_queue_url=submit_queue) == 1

        assert _messages(submit_queue) == [
            {
                "acquisition_date": ACQUISITION_DATE,
                "source_granule_ids": [SAFE_ID],
                "output_granule_id": GRANULE_ID_STR,
                "attempt": 1,
            }
        ]

    def test_twin_granules_are_one_message(
        self, store: S3RecordStore, submit_queue: str
    ) -> None:
        _await(store, [SAFE_ID, TWIN_SAFE_ID])

        assert process_aux_event(s3_key=_AUX_KEY, submit_queue_url=submit_queue) == 1

        [message] = _messages(submit_queue)
        assert message["source_granule_ids"] == [SAFE_ID, TWIN_SAFE_ID]
