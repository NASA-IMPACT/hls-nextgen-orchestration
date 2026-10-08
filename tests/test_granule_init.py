"""Tests for the granule_init Lambda."""

import json
from typing import Any
from unittest.mock import MagicMock

import pytest
from aws_lambda_powertools.utilities.typing import LambdaContext

from common import S3RecordStore
from granule_init.handler import (
    check_aux_data,
    convert_safe_id_to_hls_id,
    detect_twin_safe_ids,
    extract_safe_id_from_s3_key,
    parse_s3_sns_message,
    process_record,
)
from tests.conftest import ACQUISITION_DATE, GRANULE_ID_STR, SAFE_ID


@pytest.fixture
def store(bucket: str) -> S3RecordStore:
    return S3RecordStore(bucket=bucket)


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


class TestConvertSafeIdToHlsId:
    def test_known_conversion(self) -> None:
        result = convert_safe_id_to_hls_id(SAFE_ID)
        assert result == GRANULE_ID_STR

    @pytest.mark.parametrize(
        ["safe_id", "expected_tile"],
        [
            ("S2A_MSIL1C_20230817T154921_N0509_R011_T18TYN_20230817T204510", "T18TYN"),
            ("S2B_MSIL1C_20240115T160901_N0509_R097_T10SEG_20240115T180000", "T10SEG"),
        ],
    )
    def test_tile_extraction(self, safe_id: str, expected_tile: str) -> None:
        hls_id = convert_safe_id_to_hls_id(safe_id)
        assert f".{expected_tile}." in hls_id


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
        records = parse_s3_sns_message(sns_msg)
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


class TestProcessRecord:
    def test_writes_awaiting_when_no_aux(
        self,
        store: S3RecordStore,
        sentinel_bucket: str,
        output_bucket: str,
        batch_queue_name: str,
        monkeypatch: pytest.MonkeyPatch,
        mocked_batch_client_submit_job: MagicMock,
        mocked_active_jobs_below_threshold: MagicMock,
    ) -> None:
        # Create an empty aux bucket so check_aux_data returns False
        import boto3

        empty_aux = "test-aux-empty"
        boto3.client("s3", region_name="us-west-2").create_bucket(
            Bucket=empty_aux,
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
        monkeypatch.setenv("AUX_DATA_BUCKET_NAME", empty_aux)
        monkeypatch.setenv("SENTINEL_JOB_DEFINITION_NAME", "sentinel-job-def")
        monkeypatch.setenv("MAX_ACTIVE_JOBS", "10000")

        import boto3

        s3 = boto3.client("s3", region_name="us-west-2")
        s3.put_object(Bucket=sentinel_bucket, Key=f"input/{SAFE_ID}.zip", Body=b"")

        process_record(_make_sqs_body())

        key = S3RecordStore.canonical_key("sentinel", ACQUISITION_DATE, SAFE_ID, 0)
        record = json.loads(s3.get_object(Bucket=store.bucket, Key=key)["Body"].read())
        assert record["current_state"] == "AWAITING"
        mocked_batch_client_submit_job.assert_not_called()

    def test_writes_submitted_when_aux_available(
        self,
        store: S3RecordStore,
        sentinel_bucket: str,
        aux_bucket: str,
        output_bucket: str,
        batch_queue_name: str,
        monkeypatch: pytest.MonkeyPatch,
        mocked_batch_client_submit_job: MagicMock,
        mocked_active_jobs_below_threshold: MagicMock,
    ) -> None:
        monkeypatch.setenv("SENTINEL_JOB_DEFINITION_NAME", "sentinel-job-def")
        monkeypatch.setenv("MAX_ACTIVE_JOBS", "10000")

        import boto3

        s3 = boto3.client("s3", region_name="us-west-2")
        s3.put_object(Bucket=sentinel_bucket, Key=f"input/{SAFE_ID}.zip", Body=b"")

        process_record(_make_sqs_body())

        key = S3RecordStore.canonical_key("sentinel", ACQUISITION_DATE, SAFE_ID, 0)
        record = json.loads(s3.get_object(Bucket=store.bucket, Key=key)["Body"].read())
        assert record["current_state"] == "SUBMITTED"
        mocked_batch_client_submit_job.assert_called_once()
