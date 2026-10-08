"""Tests for the granule_init Lambda."""

import json
from typing import Any
from unittest.mock import MagicMock

import pytest
from aws_lambda_powertools.utilities.typing import LambdaContext
from batch_event_job_monitor import S3RecordStore

from common.ancillary import check_aux_data
from common.jobs import AWAITING_ANCILLARY, SUBMITTED, sentinel_job_group
from granule_init.handler import (
    detect_twin_safe_ids,
    extract_safe_id_from_s3_key,
    parse_s3_sqs_message,
    process_record,
)
from tests.conftest import ACQUISITION_DATE, GRANULE_ID_STR, SAFE_ID


@pytest.fixture
def lambda_context() -> LambdaContext:
    ctx = MagicMock(spec=LambdaContext)
    ctx.function_name = "test-granule-init"
    ctx.memory_limit_in_mb = 512
    ctx.invoked_function_arn = "arn:aws:lambda:us-west-2:123456789012:function:test"
    ctx.aws_request_id = "test-request-id"
    return ctx


# ---------------------------------------------------------------------------
# Unit helpers
# ---------------------------------------------------------------------------


class TestExtractSafeId:
    def test_extracts_from_key(self) -> None:
        key = f"input/{SAFE_ID}.zip"
        assert extract_safe_id_from_s3_key(key) == SAFE_ID

    def test_extracts_no_extension(self) -> None:
        key = f"input/{SAFE_ID}"
        assert extract_safe_id_from_s3_key(key) == SAFE_ID


class TestParseSnsMessage:
    def test_round_trip(self) -> None:
        s3_event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "test"},
                        "object": {"key": f"input/{SAFE_ID}.zip"},
                    }
                }
            ]
        }
        sns_msg = json.dumps({"Message": json.dumps(s3_event)})
        records = parse_s3_sqs_message(sns_msg)
        assert len(records) == 1
        assert records[0]["s3"]["object"]["key"] == f"input/{SAFE_ID}.zip"


class TestDetectTwinSafeIds:
    def test_single_granule(self, sentinel_bucket: str) -> None:
        import boto3

        s3 = boto3.client("s3", region_name="us-west-2")
        s3.put_object(Bucket=sentinel_bucket, Key=f"input/{SAFE_ID}.zip", Body=b"")

        ids = detect_twin_safe_ids(sentinel_bucket, SAFE_ID, s3)
        assert ids == [SAFE_ID]

    def test_twin_granules(self, sentinel_bucket: str) -> None:
        import boto3

        s3 = boto3.client("s3", region_name="us-west-2")
        twin_id = "S2A_MSIL1C_20230817T154921_N0509_R011_T18TYN_20230818T090000"
        s3.put_object(Bucket=sentinel_bucket, Key=f"input/{SAFE_ID}.zip", Body=b"")
        s3.put_object(Bucket=sentinel_bucket, Key=f"input/{twin_id}.zip", Body=b"")

        ids = detect_twin_safe_ids(sentinel_bucket, SAFE_ID, s3)
        assert len(ids) == 2
        assert SAFE_ID in ids
        assert twin_id in ids


class TestCheckAuxData:
    def test_returns_true_when_aux_present(
        self, granule_id: Any, aux_bucket: str
    ) -> None:
        import boto3

        s3 = boto3.client("s3", region_name="us-west-2")
        assert check_aux_data(granule_id, aux_bucket, s3)

    def test_returns_false_when_aux_absent(self, granule_id: Any, s3: Any) -> None:
        import uuid

        fake_bucket = f"empty-{uuid.uuid4().hex}"
        s3.create_bucket(
            Bucket=fake_bucket,
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
        assert not check_aux_data(granule_id, fake_bucket, s3)


# ---------------------------------------------------------------------------
# Integration: process_record
# ---------------------------------------------------------------------------


def _make_sqs_body(safe_id: str = SAFE_ID, bucket: str = "test-sentinel") -> str:
    s3_event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": f"input/{safe_id}.zip"},
                }
            }
        ]
    }
    return json.dumps({"Message": json.dumps(s3_event)})


def _pointer_exists(store: S3RecordStore, state: Any) -> bool:
    import boto3

    [context] = sentinel_job_group(
        acquisition_date=ACQUISITION_DATE,
        source_granule_ids=[SAFE_ID],
        output_granule_id=GRANULE_ID_STR,
    ).contexts()
    key = store.state_pointer_key(state, context)
    s3 = boto3.client("s3", region_name="us-west-2")
    return bool(s3.list_objects_v2(Bucket=store.bucket, Prefix=key).get("KeyCount"))


class TestProcessRecord:
    @pytest.fixture(autouse=True)
    def env(
        self,
        sentinel_bucket: str,
        output_bucket: str,
        batch_queue_name: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import boto3

        monkeypatch.setenv("SENTINEL_JOB_DEFINITION_NAME", "sentinel-job-def")
        monkeypatch.setenv("MAX_ACTIVE_JOBS", "10000")
        boto3.client("s3", region_name="us-west-2").put_object(
            Bucket=sentinel_bucket, Key=f"input/{SAFE_ID}.zip", Body=b""
        )

    def test_writes_awaiting_ancillary_when_no_aux(
        self,
        store: S3RecordStore,
        monkeypatch: pytest.MonkeyPatch,
        mocked_submit_job: MagicMock,
        mocked_active_jobs_below_threshold: MagicMock,
    ) -> None:
        import boto3

        empty_aux = "test-aux-empty"
        boto3.client("s3", region_name="us-west-2").create_bucket(
            Bucket=empty_aux,
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
        monkeypatch.setenv("AUX_DATA_BUCKET_NAME", empty_aux)

        process_record(_make_sqs_body())

        assert _pointer_exists(store, AWAITING_ANCILLARY)
        mocked_submit_job.assert_not_called()

    def test_writes_awaiting_ancillary_when_too_many_jobs_are_active(
        self,
        store: S3RecordStore,
        aux_bucket: str,
        mocked_submit_job: MagicMock,
        mocked_active_jobs_below_threshold: MagicMock,
    ) -> None:
        mocked_active_jobs_below_threshold.return_value = False

        process_record(_make_sqs_body())

        assert _pointer_exists(store, AWAITING_ANCILLARY)
        mocked_submit_job.assert_not_called()

    def test_claims_and_submits_when_aux_available(
        self,
        store: S3RecordStore,
        aux_bucket: str,
        mocked_submit_job: MagicMock,
        mocked_active_jobs_below_threshold: MagicMock,
    ) -> None:
        process_record(_make_sqs_body())

        assert _pointer_exists(store, SUBMITTED)
        mocked_submit_job.assert_called_once()
        job_group = mocked_submit_job.call_args.kwargs["job_group"]
        assert job_group.input_entity_ids == [SAFE_ID]
        assert job_group.attempt == 1

    def test_a_duplicate_event_is_not_submitted_twice(
        self,
        store: S3RecordStore,
        aux_bucket: str,
        mocked_submit_job: MagicMock,
        mocked_active_jobs_below_threshold: MagicMock,
    ) -> None:
        process_record(_make_sqs_body())
        process_record(_make_sqs_body())

        mocked_submit_job.assert_called_once()
