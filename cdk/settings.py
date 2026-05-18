import datetime as dt
from typing import Annotated, Any, Literal

from pydantic import BeforeValidator, model_validator
from pydantic_settings import BaseSettings


def include_trailing_slash(value: Any) -> Any:
    """Make sure the value includes a trailing slash if str"""
    if isinstance(value, str):
        return value.rstrip("/") + "/"
    return value


class StackSettings(BaseSettings):
    """Deployment settings for HLS processing."""

    STACK_NAME: str
    STAGE: Literal["dev", "prod"]

    @model_validator(mode="before")
    @classmethod
    def apply_stage_defaults(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "STAGE" not in data:
                raise ValueError("STAGE is required")
            stage = data["STAGE"]

            if not data.get("STACK_NAME"):
                data["STACK_NAME"] = f"hls-nextgen-orchestration-{stage}"

            if not data.get("ANCILLARY_TRIGGER_QUEUE_NAME"):
                data["ANCILLARY_TRIGGER_QUEUE_NAME"] = (
                    f"hls-orch-ancillary-queue-{stage}"
                )

            if not data.get("ANCILLARY_SUBMIT_QUEUE_NAME"):
                data["ANCILLARY_SUBMIT_QUEUE_NAME"] = (
                    f"hls-orch-ancillary-submit-{stage}"
                )

            if not data.get("ANCILLARY_SUBMIT_DLQ_NAME"):
                data["ANCILLARY_SUBMIT_DLQ_NAME"] = (
                    f"hls-orch-ancillary-submit-dlq-{stage}"
                )

            if not data.get("ATHENA_DATABASE_NAME"):
                data["ATHENA_DATABASE_NAME"] = f"hls-nextgen-orchestration-{stage}"

            if not data.get("JOB_RETRY_QUEUE_NAME"):
                data["JOB_RETRY_QUEUE_NAME"] = f"hls-orch-retry-{stage}"

            if not data.get("JOB_FAILURE_DLQ_NAME"):
                data["JOB_FAILURE_DLQ_NAME"] = f"hls-orch-failure-{stage}"

            if not data.get("PROCESSING_LOG_GROUP_NAME"):
                data["PROCESSING_LOG_GROUP_NAME"] = (
                    f"hls-science-container-logs-{stage}"
                )

        return data

    MCP_ACCOUNT_ID: str
    MCP_ACCOUNT_REGION: str = "us-west-2"
    MCP_IAM_PERMISSION_BOUNDARY_ARN: str | None = None

    VPC_ID: str

    # ----- Credentials for LPDAAC bucket
    # By default we use our own IAM role that has read permissions on LPDAAC side
    # in their bucket policies. If this has been removed or has issues, we can fall back
    # to using the DAAC `/s3credentials` endpoint to provide temporary credentials.
    #
    # We expect this credential to exist in SecretsManager already!

    # Whether to enable use and scheduling of credential rotation.

    # ----- Buckets
    PROCESSING_BUCKET_NAME: str

    SENTINEL_BUCKET_NAME: str

    AUX_DATA_BUCKET_NAME: str

    # Output bucket for processed products
    OUTPUT_BUCKET_NAME: str

    # Debug bucket (optional, but useful for avoiding triggering LPDAAC ingest)
    DEBUG_BUCKET_NAME: str | None = None

    # ----- HLS processing
    SENTINEL_CONTAINER_ECR_URI: str
    # Job vCPU and memory limits
    SENTINEL_JOB_VCPU: int = 1
    SENTINEL_JOB_MEMORY_MB: int = 2_000
    # Custom log group (otherwise they'll land in the catch-all AWS Batch log group)
    PROCESSING_LOG_GROUP_NAME: str
    # Number of internal AWS Batch job retries
    PROCESSING_JOB_RETRY_ATTEMPTS: int = 3

    # AWS Batch cluster reference to SSM parameter describing the AMI _or_ the AMI ID
    # If using SSM to resolve the AMI ID, prefix with `resolve:ssm`.
    # MCP_AMI_ID: str = "resolve:ssm:/mcp/amis/aml2023-ecs"
    MCP_AMI_ID: str = (
        "resolve:ssm:/aws/service/ecs/optimized-ami"
        "/amazon-linux-2023/recommended/image_id"
    )

    # Cluster instance classes
    BATCH_INSTANCE_CLASSES: list[str] = [
        "C4",
        "C5",
        "C5A",
        "C6A",
        "C6I",
    ]

    # Cluster scaling max
    BATCH_MAX_VCPU: int = 10

    MAX_ACTIVE_JOBS: int = 10_000

    # ----- Job retry system
    # Send retryable failed AWS Batch jobs to this queue
    JOB_RETRY_QUEUE_NAME: str
    # Failed AWS Batch jobs go to a DLQ that can redrive to the retry queue
    JOB_FAILURE_DLQ_NAME: str

    # ----- Ancillary trigger
    # SQS queue that receives S3 event notifications from the aux data bucket
    ANCILLARY_TRIGGER_QUEUE_NAME: str
    # Internal SQS queue for per-granule submission work fanned out from the trigger
    ANCILLARY_SUBMIT_QUEUE_NAME: str
    # DLQ for the ancillary-submit queue
    ANCILLARY_SUBMIT_DLQ_NAME: str

    # ----- State-pointer inventory (state/ prefix → daily S3 inventory)
    STATE_INVENTORY_PREFIX: Annotated[str, BeforeValidator(include_trailing_slash)] = (
        "state-inventories/"
    )

    # ----- Athena database (shared by records and state tables)
    ATHENA_DATABASE_NAME: str

    # ----- Records Athena database
    ATHENA_RECORDS_TABLE_START_DATE: str = "2026-05-01"
    ATHENA_RECORDS_SENTINEL_TABLE_NAME: str = "records_sentinel"

    # ----- State Athena database (S3 inventory over state/ prefix)
    ATHENA_STATE_TABLE_START_DATETIME: dt.datetime = dt.datetime(2026, 5, 1)
    ATHENA_STATE_TABLE_NAME: str = "state_inventory"
    ATHENA_STATE_VIEW_NAME: str = "current_granule_states"

    # ----- Phase 0 shadow observability (Sentinel-2 only)
    # Set these to shadow the existing Step Functions Sentinel-2 AC Batch jobs.
    # Landsat (AC + tile) is out of scope for Phase 0.
    # Both must be set together or not at all.
    PHASE0_BATCH_QUEUE_ARN: str | None = None
    PHASE0_SENTINEL_JOB_DEFINITION_NAME: str | None = None

    @model_validator(mode="after")
    def validate_phase0_settings(self) -> "StackSettings":
        defined = [
            getattr(self, k, None) is not None
            for k in ("PHASE0_BATCH_QUEUE_ARN", "PHASE0_SENTINEL_JOB_DEFINITION_NAME")
        ]
        if any(defined) and not all(defined):
            raise ValueError("Partial Phase 0 configuration. Set all or nothing.")
        return self
