import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws
from mypy_boto3_batch import BatchClient
from mypy_boto3_batch.type_defs import JobDetailTypeDef
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient

from common.aws_batch import AwsBatchClient, JobChangeEvent
from common.models import GranuleId, GranuleProcessingEvent

# Set metrics namespace before any modules are imported
os.environ["POWERTOOLS_METRICS_NAMESPACE"] = "test-namespace"

FIXTURES = Path(__file__).parent / "fixtures"

# ---------------------------------------------------------------------------
# Example granule data
# ---------------------------------------------------------------------------

SAFE_ID = "S2A_MSIL1C_20230817T154921_N0509_R011_T18TYN_20230817T204510"
GRANULE_ID_STR = "HLS.S30.T18TYN.2023229T154921.v2.0"
ACQUISITION_DATE = "2023-08-17"


@pytest.fixture
def granule_id() -> GranuleId:
    return GranuleId.from_str(GRANULE_ID_STR)


@pytest.fixture
def source_granule_id() -> str:
    return SAFE_ID


@pytest.fixture
def granule_processing_event() -> GranuleProcessingEvent:
    return GranuleProcessingEvent(
        workflow="sentinel",
        acquisition_date=ACQUISITION_DATE,
        source_granule_ids=[SAFE_ID],
        output_granule_id=GRANULE_ID_STR,
        attempt=0,
    )


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
    return "test-processing"


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


@pytest.fixture
def batch_job_definition(monkeypatch: pytest.MonkeyPatch) -> str:
    job_definition_name = "hls-processing"
    monkeypatch.setenv("BATCH_JOB_DEFINITION_NAME", job_definition_name)
    return job_definition_name


# ---------------------------------------------------------------------------
# Batch job detail fixtures
# ---------------------------------------------------------------------------


LOG_STREAM_NAME = "test-job-definition/default/abcdef0123456789abcdef0123456789"
LOG_GROUP_NAME = "hls-nextgen-orchestration-processing-dev"


def _make_job_detail(
    exit_code: int | None,
    status_reason: str = "Essential container in task exited",
    attempts: int = 1,
    max_attempts: int = 3,
    env: list[dict[str, str]] | None = None,
    created_at: int = 1745434620000,
    stopped_at: int = 1745434913000,
) -> JobDetailTypeDef:
    """Build a minimal JobDetailTypeDef for testing."""
    container_env = env or [
        {"name": "WORKFLOW", "value": "sentinel"},
        {"name": "ACQUISITION_DATE", "value": ACQUISITION_DATE},
        {"name": "SOURCE_GRANULE_IDS", "value": SAFE_ID},
        {"name": "OUTPUT_GRANULE_ID", "value": GRANULE_ID_STR},
        {"name": "ATTEMPT", "value": "0"},
    ]
    container: dict = {
        "environment": container_env,
        "exitCode": exit_code,
        "logStreamName": LOG_STREAM_NAME,
        "logConfiguration": {
            "logDriver": "awslogs",
            "options": {
                "awslogs-group": LOG_GROUP_NAME,
                "awslogs-region": "us-west-2",
                "awslogs-stream-prefix": "job",
            },
            "secretOptions": [],
        },
    }
    if exit_code is None:
        del container["exitCode"]

    return cast(
        JobDetailTypeDef,
        {
            "jobId": "test-job-id-123",
            "jobName": "test-job",
            "jobQueue": "arn:aws:batch:us-west-2:123456789012:job-queue/test",
            "status": "FAILED" if exit_code != 0 else "SUCCEEDED",
            "statusReason": status_reason,
            "createdAt": created_at,
            "stoppedAt": stopped_at,
            "attempts": [
                {"container": {"exitCode": exit_code, "logStreamName": LOG_STREAM_NAME}}
                for _ in range(attempts)
            ],
            "retryStrategy": {"attempts": max_attempts},
            "container": container,
        },
    )


@pytest.fixture
def job_detail_success() -> JobDetailTypeDef:
    return _make_job_detail(exit_code=0)


@pytest.fixture
def job_detail_failed_error() -> JobDetailTypeDef:
    return _make_job_detail(exit_code=1)


@pytest.fixture
def job_detail_cloudy() -> JobDetailTypeDef:
    return _make_job_detail(exit_code=4)


@pytest.fixture
def job_detail_low_sun() -> JobDetailTypeDef:
    return _make_job_detail(exit_code=3)


@pytest.fixture
def job_detail_failed_spot() -> JobDetailTypeDef:
    return _make_job_detail(
        exit_code=None,
        status_reason="Host EC2 (instance i-0123456789) terminated.",
        attempts=3,
        max_attempts=3,
    )


@pytest.fixture
def job_detail_failed_spot_nonfinal() -> JobDetailTypeDef:
    return _make_job_detail(
        exit_code=None,
        status_reason="Host EC2 (instance i-0123456789) terminated.",
        attempts=1,
        max_attempts=3,
    )


def _wrap_detail(detail: JobDetailTypeDef) -> JobChangeEvent:
    return cast(
        JobChangeEvent,
        {
            "version": "0",
            "id": "test-event-id",
            "detail-type": "Batch Job State Change",
            "source": "aws.batch",
            "account": "123456789012",
            "time": "2025-04-23T19:01:54Z",
            "region": "us-west-2",
            "resources": [],
            "detail": detail,
        },
    )


@pytest.fixture
def event_job_success(job_detail_success: JobDetailTypeDef) -> JobChangeEvent:
    return _wrap_detail(job_detail_success)


@pytest.fixture
def event_job_failed_error(job_detail_failed_error: JobDetailTypeDef) -> JobChangeEvent:
    return _wrap_detail(job_detail_failed_error)


@pytest.fixture
def event_job_cloudy(job_detail_cloudy: JobDetailTypeDef) -> JobChangeEvent:
    return _wrap_detail(job_detail_cloudy)


@pytest.fixture
def event_job_low_sun(job_detail_low_sun: JobDetailTypeDef) -> JobChangeEvent:
    return _wrap_detail(job_detail_low_sun)


@pytest.fixture
def event_job_failed_spot(job_detail_failed_spot: JobDetailTypeDef) -> JobChangeEvent:
    return _wrap_detail(job_detail_failed_spot)


@pytest.fixture
def event_job_failed_spot_nonfinal(
    job_detail_failed_spot_nonfinal: JobDetailTypeDef,
) -> JobChangeEvent:
    return _wrap_detail(job_detail_failed_spot_nonfinal)


# Phase 0 shadow fixture (uses GRANULE_LIST env var, no WORKFLOW)
@pytest.fixture
def event_job_shadow_sentinel() -> JobChangeEvent:
    detail = _make_job_detail(
        exit_code=0,
        env=[
            {"name": "GRANULE_LIST", "value": SAFE_ID},
            {"name": "ATTEMPT", "value": "0"},
        ],
    )
    return _wrap_detail(detail)


# ---------------------------------------------------------------------------
# AwsBatchClient mocks
# ---------------------------------------------------------------------------


@pytest.fixture
def mocked_batch_client_submit_job() -> Iterator[MagicMock]:
    with patch.object(AwsBatchClient, "submit_job", return_value="foo-job-id") as mock:
        yield mock


@pytest.fixture
def mocked_active_jobs_below_threshold() -> Iterator[MagicMock]:
    with patch.object(
        AwsBatchClient, "active_jobs_below_threshold", return_value=True
    ) as mock:
        yield mock
