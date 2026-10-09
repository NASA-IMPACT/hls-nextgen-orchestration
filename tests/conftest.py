import json
import os
from collections.abc import Iterator
from typing import cast
from unittest.mock import MagicMock, patch

import boto3
import pytest
from batch_event_job_monitor import S3RecordStore
from moto import mock_aws
from mypy_boto3_batch import BatchClient
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient

from common.aws_batch import AwsBatchClient
from common.models import GranuleId

# Set metrics namespace before any modules are imported
os.environ["POWERTOOLS_METRICS_NAMESPACE"] = "test-namespace"

# ---------------------------------------------------------------------------
# Example granule data
# ---------------------------------------------------------------------------

SAFE_ID = "S2A_MSIL1C_20230817T154921_N0509_R011_T18TYN_20230817T204510"
GRANULE_ID_STR = "HLS.S30.T18TYN.2023229T154921.v2.0"
ACQUISITION_DATE = "2023-08-17"
TWIN_SAFE_ID = "S2A_MSIL1C_20230817T154921_N0509_R011_T18TYN_20230818T090000"

PROCESSING_KEY_PREFIX = "monitoring/"


@pytest.fixture
def granule_id() -> GranuleId:
    return GranuleId.from_str(GRANULE_ID_STR)


@pytest.fixture
def source_granule_id() -> str:
    return SAFE_ID


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    settings = {
        "JOB_RETRY_QUEUE_NAME": "hls-orch-job-retries",
        "JOB_FAILURE_DLQ_NAME": "hls-orch-job-failure-dlq",
    }
    for key, value in settings.items():
        monkeypatch.setenv(key, value)
    return settings


# ---------------------------------------------------------------------------
# AWS credentials / mocking
# ---------------------------------------------------------------------------


@pytest.fixture
def aws_credentials() -> None:
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------


@pytest.fixture
def s3(aws_credentials: None) -> Iterator[S3Client]:
    with mock_aws():
        yield boto3.client("s3", region_name="us-west-2")


@pytest.fixture
def bucket(s3: S3Client, monkeypatch: pytest.MonkeyPatch) -> str:
    s3.create_bucket(
        Bucket="test-processing",
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
    )
    monkeypatch.setenv("PROCESSING_BUCKET_NAME", "test-processing")
    monkeypatch.setenv("PROCESSING_KEY_PREFIX", PROCESSING_KEY_PREFIX)
    return "test-processing"


@pytest.fixture
def store(bucket: str) -> S3RecordStore:
    return S3RecordStore(bucket=bucket, key_prefix=PROCESSING_KEY_PREFIX)


@pytest.fixture
def sentinel_bucket(s3: S3Client, monkeypatch: pytest.MonkeyPatch) -> str:
    s3.create_bucket(
        Bucket="test-sentinel",
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
    )
    monkeypatch.setenv("SENTINEL_BUCKET_NAME", "test-sentinel")
    return "test-sentinel"


@pytest.fixture
def output_bucket(s3: S3Client, monkeypatch: pytest.MonkeyPatch) -> str:
    s3.create_bucket(
        Bucket="test-outputs",
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
    )
    monkeypatch.setenv("OUTPUT_BUCKET_NAME", "test-outputs")
    return "test-outputs"


@pytest.fixture
def aux_bucket(
    s3: S3Client, granule_id: GranuleId, monkeypatch: pytest.MonkeyPatch
) -> str:
    s3.create_bucket(
        Bucket="test-aux",
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
    )
    year = granule_id.begin_datetime.strftime("%Y")
    ydoy = granule_id.begin_datetime.strftime("%Y%j")
    s3.put_object(
        Bucket="test-aux",
        Key=f"lasrc_aux/LADS/{year}/VJ104ANC.A{ydoy}",
        Body=b"",
    )
    monkeypatch.setenv("AUX_DATA_BUCKET_NAME", "test-aux")
    return "test-aux"


# ---------------------------------------------------------------------------
# SQS
# ---------------------------------------------------------------------------


@pytest.fixture
def sqs(aws_credentials: None) -> Iterator[SQSClient]:
    with mock_aws():
        yield boto3.client("sqs", region_name="us-west-2")


def _queue_url_to_arn(sqs_client: SQSClient, url: str) -> str:
    resp = sqs_client.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])
    return cast(str, resp["Attributes"]["QueueArn"])


@pytest.fixture
def retry_queue(
    sqs: SQSClient,
    failure_dlq: str,
    settings: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[str]:
    queue_name = os.environ["JOB_RETRY_QUEUE_NAME"]
    failure_dlq_arn = _queue_url_to_arn(sqs, failure_dlq)
    queue_url = sqs.create_queue(
        QueueName=queue_name,
        Attributes={
            "RedrivePolicy": json.dumps(
                {"deadLetterTargetArn": failure_dlq_arn, "maxReceiveCount": 1}
            )
        },
    )["QueueUrl"]
    monkeypatch.setenv("JOB_RETRY_QUEUE_URL", queue_url)
    yield queue_url
    sqs.delete_queue(QueueUrl=queue_url)


@pytest.fixture
def failure_dlq(
    sqs: SQSClient, settings: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> Iterator[str]:
    queue_name = os.environ["JOB_FAILURE_DLQ_NAME"]
    queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    monkeypatch.setenv("JOB_FAILURE_DLQ_URL", queue_url)
    yield queue_url
    sqs.delete_queue(QueueUrl=queue_url)


# ---------------------------------------------------------------------------
# AWS Batch
# ---------------------------------------------------------------------------


@pytest.fixture
def batch(aws_credentials: None) -> Iterator[BatchClient]:
    with mock_aws():
        yield boto3.client("batch", region_name="us-west-2")


@pytest.fixture
def batch_queue_name(monkeypatch: pytest.MonkeyPatch) -> str:
    queue_name = "hls-processing"
    monkeypatch.setenv("BATCH_QUEUE_NAME", queue_name)
    return queue_name


# ---------------------------------------------------------------------------
# Batch mocks
# ---------------------------------------------------------------------------


@pytest.fixture
def mocked_submit_job() -> Iterator[MagicMock]:
    with patch("common.jobs.submit_job", return_value="foo-job-id") as mock:
        yield mock


@pytest.fixture
def mocked_active_jobs_below_threshold() -> Iterator[MagicMock]:
    with patch.object(
        AwsBatchClient, "active_jobs_below_threshold", return_value=True
    ) as mock:
        yield mock
