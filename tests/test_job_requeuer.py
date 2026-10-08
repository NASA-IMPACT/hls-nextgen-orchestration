"""Tests for the job_requeuer Lambda."""

import json
from typing import TYPE_CHECKING, cast

import pytest

from common import GranuleProcessingEvent, ProcessingState, S3RecordStore
from job_requeuer.handler import job_requeuer
from tests.conftest import ACQUISITION_DATE, GRANULE_ID_STR, SAFE_ID

if TYPE_CHECKING:
    from aws_lambda_typing.events import SQSEvent


@pytest.fixture
def store(bucket: str) -> S3RecordStore:
    return S3RecordStore(bucket=bucket)


def _make_sqs_event(events: list[GranuleProcessingEvent]) -> "SQSEvent":
    return cast("SQSEvent", {"Records": [{"body": e.to_json()} for e in events]})


class TestJobRequeuer:
    def test_increments_attempt_and_writes_awaiting(
        self,
        store: S3RecordStore,
        mocked_batch_client_submit_job: object,
        batch_queue_name: str,
        batch_job_definition: str,
        output_bucket: str,
    ) -> None:
        event = GranuleProcessingEvent(
            workflow="sentinel",
            acquisition_date=ACQUISITION_DATE,
            source_granule_ids=[SAFE_ID],
            output_granule_id=GRANULE_ID_STR,
            attempt=0,
        )
        job_requeuer(
            job_queue=batch_queue_name,
            job_definition_name=batch_job_definition,
            output_bucket=output_bucket,
            processing_bucket=store.bucket,
            event=_make_sqs_event([event]),
        )

        import boto3

        s3 = boto3.client("s3", region_name="us-west-2")
        key = S3RecordStore.canonical_key("sentinel", ACQUISITION_DATE, SAFE_ID, 1)
        record = json.loads(s3.get_object(Bucket=store.bucket, Key=key)["Body"].read())
        assert record["attempt"] == 1
        assert record["current_state"] == "AWAITING"

        pointer_key = S3RecordStore.state_pointer_key(
            ProcessingState.AWAITING, "sentinel", ACQUISITION_DATE, SAFE_ID, 1
        )
        resp = s3.list_objects_v2(Bucket=store.bucket, Prefix=pointer_key)
        assert resp.get("KeyCount", 0) == 1

    def test_multiple_records(
        self,
        store: S3RecordStore,
        mocked_batch_client_submit_job: object,
        batch_queue_name: str,
        batch_job_definition: str,
        output_bucket: str,
    ) -> None:
        events = [
            GranuleProcessingEvent(
                workflow="sentinel",
                acquisition_date=ACQUISITION_DATE,
                source_granule_ids=[f"S2A_src_{i}"],
                output_granule_id=f"HLS.S30.T18TYN.2023229T15492{i}.v2.0",
                attempt=0,
            )
            for i in range(3)
        ]
        job_requeuer(
            job_queue=batch_queue_name,
            job_definition_name=batch_job_definition,
            output_bucket=output_bucket,
            processing_bucket=store.bucket,
            event=_make_sqs_event(events),
        )

        import boto3

        s3 = boto3.client("s3", region_name="us-west-2")
        resp = s3.list_objects_v2(Bucket=store.bucket, Prefix="records/sentinel/")
        assert resp.get("KeyCount", 0) == 3
