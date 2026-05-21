"""Tests for S3RecordStore (three-object schema)."""

import json

import boto3
import pytest
from mypy_boto3_s3 import S3Client

from common import ProcessingState, S3RecordStore
from common.record_store import ProcessingEventRecord

WORKFLOW = "sentinel"
ACQ_DATE = "2024-01-15"
SRC_ID = "S2A_MSIL1C_20240115T..."
OUTPUT_ID = "HLS.S30.T10SEG.2024015T160901.v2.0"
ATTEMPT = 0


@pytest.fixture
def store(bucket: str) -> S3RecordStore:
    return S3RecordStore(bucket=bucket)


class TestKeyConstructors:
    def test_canonical_key(self) -> None:
        key = S3RecordStore.canonical_key(WORKFLOW, ACQ_DATE, SRC_ID, ATTEMPT)
        assert key == (
            f"records/workflow={WORKFLOW}/acquisition_date={ACQ_DATE}"
            f"/source_granule_id={SRC_ID}/{ATTEMPT:03d}.json"
        )

    def test_state_pointer_key(self) -> None:
        key = S3RecordStore.state_pointer_key(
            ProcessingState.AWAITING, WORKFLOW, ACQ_DATE, SRC_ID, ATTEMPT
        )
        assert key.startswith(
            f"state/state=AWAITING/workflow={WORKFLOW}"
            f"/acquisition_date={ACQ_DATE}/source_granule_id={SRC_ID}/"
        )

    def test_output_index_key(self) -> None:
        key = S3RecordStore.output_index_key(
            ProcessingState.SUCCESS, WORKFLOW, ACQ_DATE, OUTPUT_ID
        )
        assert key == (
            f"outputs/state=SUCCESS/workflow={WORKFLOW}"
            f"/acquisition_date={ACQ_DATE}/{OUTPUT_ID}"
        )


class TestAppendCanonicalEvent:
    def test_creates_new_record(self, store: S3RecordStore) -> None:
        s3 = boto3.client("s3", region_name="us-west-2")
        event = ProcessingEventRecord(state="AWAITING", ts="2024-01-15T00:00:00Z")
        store.append_canonical_event(
            source_granule_id=SRC_ID,
            output_granule_id=OUTPUT_ID,
            workflow=WORKFLOW,
            acquisition_date=ACQ_DATE,
            attempt=ATTEMPT,
            event=event,
            shadow=False,
        )
        key = S3RecordStore.canonical_key(WORKFLOW, ACQ_DATE, SRC_ID, ATTEMPT)
        resp = s3.get_object(Bucket=store.bucket, Key=key)
        record = json.loads(resp["Body"].read())
        assert record["source_granule_id"] == SRC_ID
        assert record["current_state"] == "AWAITING"
        assert len(record["events"]) == 1
        assert record["shadow"] is False

    def test_appends_to_existing_record(self, store: S3RecordStore) -> None:
        s3 = boto3.client("s3", region_name="us-west-2")
        transitions = [
            ("AWAITING", "2024-01-15T00:00:00Z"),
            ("SUBMITTED", "2024-01-16T00:00:00Z"),
        ]
        for state_name, ts in transitions:
            store.append_canonical_event(
                source_granule_id=SRC_ID,
                output_granule_id=OUTPUT_ID,
                workflow=WORKFLOW,
                acquisition_date=ACQ_DATE,
                attempt=ATTEMPT,
                event=ProcessingEventRecord(state=state_name, ts=ts),
            )
        key = S3RecordStore.canonical_key(WORKFLOW, ACQ_DATE, SRC_ID, ATTEMPT)
        resp = s3.get_object(Bucket=store.bucket, Key=key)
        record = json.loads(resp["Body"].read())
        assert len(record["events"]) == 2
        assert record["current_state"] == "SUBMITTED"

    def test_shadow_flag(self, store: S3RecordStore) -> None:
        s3 = boto3.client("s3", region_name="us-west-2")
        store.append_canonical_event(
            source_granule_id=SRC_ID,
            output_granule_id=OUTPUT_ID,
            workflow=WORKFLOW,
            acquisition_date=ACQ_DATE,
            attempt=ATTEMPT,
            event=ProcessingEventRecord(state="SUBMITTED", ts="2024-01-15T00:00:00Z"),
            shadow=True,
        )
        key = S3RecordStore.canonical_key(WORKFLOW, ACQ_DATE, SRC_ID, ATTEMPT)
        resp = s3.get_object(Bucket=store.bucket, Key=key)
        record = json.loads(resp["Body"].read())
        assert record["shadow"] is True


class TestStatePointer:
    def test_write_and_read_pointer(self, store: S3RecordStore) -> None:
        s3 = boto3.client("s3", region_name="us-west-2")
        store.write_state_pointer(
            workflow=WORKFLOW,
            acquisition_date=ACQ_DATE,
            source_granule_id=SRC_ID,
            attempt=ATTEMPT,
            new_state=ProcessingState.AWAITING,
            old_state=None,
            output_granule_id=OUTPUT_ID,
        )
        key = S3RecordStore.state_pointer_key(
            ProcessingState.AWAITING, WORKFLOW, ACQ_DATE, SRC_ID, ATTEMPT
        )
        resp = s3.get_object(Bucket=store.bucket, Key=key)
        body = json.loads(resp["Body"].read())
        assert body["source_granule_id"] == SRC_ID
        assert body["output_granule_id"] == OUTPUT_ID

    def test_transition_deletes_old_pointer(
        self, store: S3RecordStore, s3: S3Client
    ) -> None:
        # Write AWAITING pointer
        store.write_state_pointer(
            workflow=WORKFLOW,
            acquisition_date=ACQ_DATE,
            source_granule_id=SRC_ID,
            attempt=ATTEMPT,
            new_state=ProcessingState.AWAITING,
            old_state=None,
            output_granule_id=OUTPUT_ID,
        )
        # Transition to SUBMITTED (should delete AWAITING)
        store.write_state_pointer(
            workflow=WORKFLOW,
            acquisition_date=ACQ_DATE,
            source_granule_id=SRC_ID,
            attempt=ATTEMPT,
            new_state=ProcessingState.SUBMITTED,
            old_state=ProcessingState.AWAITING,
            output_granule_id=OUTPUT_ID,
        )
        awaiting_key = S3RecordStore.state_pointer_key(
            ProcessingState.AWAITING, WORKFLOW, ACQ_DATE, SRC_ID, ATTEMPT
        )
        resp = s3.list_objects_v2(Bucket=store.bucket, Prefix=awaiting_key)
        assert resp.get("KeyCount", 0) == 0

        submitted_key = S3RecordStore.state_pointer_key(
            ProcessingState.SUBMITTED, WORKFLOW, ACQ_DATE, SRC_ID, ATTEMPT
        )
        resp = s3.list_objects_v2(Bucket=store.bucket, Prefix=submitted_key)
        assert resp.get("KeyCount", 0) == 1

    def test_conditional_write_first_succeeds(self, store: S3RecordStore) -> None:
        written = store.write_state_pointer_conditional(
            workflow=WORKFLOW,
            acquisition_date=ACQ_DATE,
            source_granule_id=SRC_ID,
            attempt=ATTEMPT,
            state=ProcessingState.SUBMITTED,
            output_granule_id=OUTPUT_ID,
        )
        assert written is True

    def test_conditional_write_second_returns_false(self, store: S3RecordStore) -> None:
        from unittest.mock import patch

        from botocore.exceptions import ClientError

        store.write_state_pointer_conditional(
            workflow=WORKFLOW,
            acquisition_date=ACQ_DATE,
            source_granule_id=SRC_ID,
            attempt=ATTEMPT,
            state=ProcessingState.SUBMITTED,
            output_granule_id=OUTPUT_ID,
        )
        # moto 5.2.0 has a bug serializing the S3 conditional-write conflict error;
        # patch put_object to raise the expected ClientError directly.
        err = {"Error": {"Code": "ConditionalRequestConflict", "Message": "conflict"}}
        conflict = ClientError(err, "PutObject")  # type: ignore[arg-type]
        with patch.object(store.client, "put_object", side_effect=conflict):
            written_again = store.write_state_pointer_conditional(
                workflow=WORKFLOW,
                acquisition_date=ACQ_DATE,
                source_granule_id=SRC_ID,
                attempt=ATTEMPT,
                state=ProcessingState.SUBMITTED,
                output_granule_id=OUTPUT_ID,
            )
        assert written_again is False

    def test_delete_state_pointer(self, store: S3RecordStore, s3: S3Client) -> None:
        store.write_state_pointer(
            workflow=WORKFLOW,
            acquisition_date=ACQ_DATE,
            source_granule_id=SRC_ID,
            attempt=ATTEMPT,
            new_state=ProcessingState.AWAITING,
            old_state=None,
            output_granule_id=OUTPUT_ID,
        )
        store.delete_state_pointer(
            workflow=WORKFLOW,
            acquisition_date=ACQ_DATE,
            source_granule_id=SRC_ID,
            attempt=ATTEMPT,
            state=ProcessingState.AWAITING,
        )
        key = S3RecordStore.state_pointer_key(
            ProcessingState.AWAITING, WORKFLOW, ACQ_DATE, SRC_ID, ATTEMPT
        )
        resp = s3.list_objects_v2(Bucket=store.bucket, Prefix=key)
        assert resp.get("KeyCount", 0) == 0


class TestOutputIndex:
    def test_write_output_index(self, store: S3RecordStore, s3: S3Client) -> None:
        store.write_output_index(
            workflow=WORKFLOW,
            acquisition_date=ACQ_DATE,
            output_granule_id=OUTPUT_ID,
            state=ProcessingState.SUCCESS,
        )
        key = S3RecordStore.output_index_key(
            ProcessingState.SUCCESS, WORKFLOW, ACQ_DATE, OUTPUT_ID
        )
        resp = s3.get_object(Bucket=store.bucket, Key=key)
        assert resp["Body"].read() == b""


class TestListAwaiting:
    def test_empty_when_no_awaiting(self, store: S3RecordStore) -> None:
        results = store.list_awaiting(WORKFLOW, ACQ_DATE)
        assert results == []

    def test_returns_awaiting_pointers(self, store: S3RecordStore) -> None:
        for i, src in enumerate(["S2A_one", "S2A_two"]):
            store.write_state_pointer(
                workflow=WORKFLOW,
                acquisition_date=ACQ_DATE,
                source_granule_id=src,
                attempt=0,
                new_state=ProcessingState.AWAITING,
                old_state=None,
                output_granule_id=f"HLS.S30.T10SEG.2024015T16000{i}.v2.0",
            )

        results = store.list_awaiting(WORKFLOW, ACQ_DATE)
        assert len(results) == 2
        src_ids = {r["source_granule_id"] for r in results}
        assert src_ids == {"S2A_one", "S2A_two"}

    def test_does_not_return_submitted(self, store: S3RecordStore) -> None:
        store.write_state_pointer(
            workflow=WORKFLOW,
            acquisition_date=ACQ_DATE,
            source_granule_id=SRC_ID,
            attempt=0,
            new_state=ProcessingState.SUBMITTED,
            old_state=None,
            output_granule_id=OUTPUT_ID,
        )
        results = store.list_awaiting(WORKFLOW, ACQ_DATE)
        assert results == []
