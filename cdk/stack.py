from typing import Any

from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_ec2 as ec2,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_python_alpha as lambda_python,
    aws_s3 as s3,
    aws_s3_notifications as s3_notifications,
    aws_sqs as sqs,
)
from constructs import Construct

from hls_constructs import (
    AthenaRecordsDatabase,
    AthenaStateDatabase,
    BatchInfra,
    BatchJob,
    ProcessingBucket,
    QueueWithDlq,
)
from settings import StackSettings

LAMBDA_EXCLUDE = [
    ".git*",
    "**/__pycache__",
    "**/*.egg-info",
    ".mypy_cache",
    ".ruff_cache",
    ".pytest_cache",
    "venv",
    ".venv",
    ".env*",
    "cdk",
    "cdk.out",
    "docs",
    "tests",
    "scripts",
]


class HlsStack(Stack):
    """HLS processing CDK stack."""

    def __init__(
        self, scope: Construct, stack_id: str, *, settings: StackSettings, **kwargs: Any
    ) -> None:
        super().__init__(scope, stack_id, **kwargs)

        if settings.MCP_IAM_PERMISSION_BOUNDARY_ARN:
            boundary = iam.ManagedPolicy.from_managed_policy_arn(
                self,
                "PermissionBoundary",
                settings.MCP_IAM_PERMISSION_BOUNDARY_ARN,
            )
            iam.PermissionsBoundary.of(self).apply(boundary)

        # ----------------------------------------------------------------------
        # Networking
        # ----------------------------------------------------------------------
        self.vpc = ec2.Vpc.from_lookup(self, "VPC", vpc_id=settings.VPC_ID)

        # ----------------------------------------------------------------------
        # Buckets
        # ----------------------------------------------------------------------
        self.aux_data_bucket = s3.Bucket.from_bucket_name(
            self,
            "AuxDataBucket",
            bucket_name=settings.AUX_DATA_BUCKET_NAME,
        )

        _processing = ProcessingBucket(
            self,
            "ProcessingBucket",
            bucket_name=settings.PROCESSING_BUCKET_NAME,
            state_inventory_prefix=settings.STATE_INVENTORY_PREFIX,
        )
        self.processing_bucket = _processing.bucket

        # FIXME: this bucket already exists, so import it post-MVP
        self.output_bucket = self._make_bucket(
            "OutputBucket", settings.OUTPUT_BUCKET_NAME
        )
        # FIXME: this bucket already exists, so import it post-MVP
        self.sentinel_bucket = self._make_bucket(
            "SentinelBucket", settings.SENTINEL_BUCKET_NAME
        )

        self.debug_bucket: s3.IBucket | None
        if settings.DEBUG_BUCKET_NAME:
            self.debug_bucket = s3.Bucket.from_bucket_name(
                self,
                "DebugBucket",
                bucket_name=settings.DEBUG_BUCKET_NAME,
            )
        else:
            self.debug_bucket = None

        # ----------------------------------------------------------------------
        # Athena databases
        # ----------------------------------------------------------------------
        self.athena_records_db = AthenaRecordsDatabase(
            self,
            "AthenaRecordsDatabase",
            database_name=settings.ATHENA_DATABASE_NAME,
            records_bucket_name=settings.PROCESSING_BUCKET_NAME,
            table_date_range_start=settings.ATHENA_RECORDS_TABLE_START_DATE,
            sentinel_table_name=settings.ATHENA_RECORDS_SENTINEL_TABLE_NAME,
        )

        self.athena_state_db = AthenaStateDatabase(
            self,
            "AthenaStateDatabase",
            database_name=settings.ATHENA_DATABASE_NAME,
            inventory_location_s3path=(
                f"s3://{settings.PROCESSING_BUCKET_NAME}"
                f"/{settings.STATE_INVENTORY_PREFIX}"
            ),
            table_datetime_start=settings.ATHENA_STATE_TABLE_START_DATETIME,
            table_name=settings.ATHENA_STATE_TABLE_NAME,
            view_name=settings.ATHENA_STATE_VIEW_NAME,
        )

        # ----------------------------------------------------------------------
        # AWS Batch infrastructure
        # ----------------------------------------------------------------------
        self.batch_infra = BatchInfra(
            self,
            "HLS-Batch-Infra",
            vpc=self.vpc,
            instance_classes=settings.BATCH_INSTANCE_CLASSES,
            max_vcpu=settings.BATCH_MAX_VCPU,
            ami_id=settings.MCP_AMI_ID,
            stage=settings.STAGE,
        )

        # ----------------------------------------------------------------------
        # HLS processing compute jobs
        # ----------------------------------------------------------------------
        self.sentinel_job = BatchJob(
            self,
            "SentinelJob",
            container_ecr_uri=settings.SENTINEL_CONTAINER_ECR_URI,
            vcpu=settings.SENTINEL_JOB_VCPU,
            memory_mb=settings.SENTINEL_JOB_MEMORY_MB,
            retry_attempts=settings.PROCESSING_JOB_RETRY_ATTEMPTS,
            log_group_name=settings.PROCESSING_LOG_GROUP_NAME,
            environment={
                "PYTHONUNBUFFERED": "TRUE",
                "SENTINEL_BUCKET_NAME": self.sentinel_bucket.bucket_name,
                "OUTPUT_BUCKET_NAME": self.output_bucket.bucket_name,
                "AUX_DATA_BUCKET_NAME": self.aux_data_bucket.bucket_name,
            },
            secrets={},
            stage=settings.STAGE,
        )

        self.output_bucket.grant_read_write(self.sentinel_job.role)
        self.sentinel_bucket.grant_read(self.sentinel_job.role)
        self.aux_data_bucket.grant_read(self.sentinel_job.role)
        if self.debug_bucket is not None:
            self.debug_bucket.grant_read(self.sentinel_job.role)

        # Shared policy for Batch job submission
        self.batch_submit_job_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            resources=[
                self.batch_infra.queue.job_queue_arn,
                self.sentinel_job.job_def_arn_without_revision,
            ],
            actions=["batch:SubmitJob"],
        )

        # ----------------------------------------------------------------------
        # Common AWS Lambda
        # ----------------------------------------------------------------------
        # FIXME: this needs to be tied to Python version & cpu arch
        self.powertools_layer = lambda_.LayerVersion.from_layer_version_arn(
            self,
            "PowertoolsLayer",
            layer_version_arn=f"arn:aws:lambda:{self.region}:017000801446:layer:AWSLambdaPowertoolsPythonV3-python312-x86_64:18",
        )

        # ----------------------------------------------------------------------
        # Job monitor & retry system
        # ----------------------------------------------------------------------
        self.job_retry = QueueWithDlq(
            self,
            "JobRetryQueue",
            queue_name=settings.JOB_RETRY_QUEUE_NAME,
            dlq_name=settings.JOB_FAILURE_DLQ_NAME,
            visibility_timeout=Duration.minutes(2),
            max_receive_count=1,
        )

        self.job_monitor_lambda = lambda_python.PythonFunction(
            self,
            "JobMonitorHandler",
            entry="src/",
            index="job_monitor/handler.py",
            handler="handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            memory_size=256,
            timeout=Duration.minutes(1),
            environment={
                "PROCESSING_BUCKET_NAME": self.processing_bucket.bucket_name,
                "BATCH_QUEUE_NAME": self.batch_infra.queue.job_queue_name,
                "JOB_RETRY_QUEUE_URL": self.job_retry.queue.queue_url,
                "JOB_FAILURE_DLQ_URL": self.job_retry.dlq.queue_url,
                "PROCESSING_JOB_RETRY_ATTEMPTS": str(
                    settings.PROCESSING_JOB_RETRY_ATTEMPTS
                ),
            },
            bundling=lambda_python.BundlingOptions(
                asset_excludes=LAMBDA_EXCLUDE,
            ),
        )
        self.processing_bucket.grant_read_write(self.job_monitor_lambda)
        self.job_retry.queue.grant_send_messages(self.job_monitor_lambda)
        self.job_retry.dlq.grant_send_messages(self.job_monitor_lambda)

        # EventBridge rule: Batch job state changes from our queue/job definition
        self.processing_job_events_rule = events.Rule(
            self,
            "ProcessingJobEventsRule",
            event_pattern=events.EventPattern(
                source=["aws.batch"],
                detail={
                    "jobQueue": [self.batch_infra.queue.job_queue_arn],
                    "jobDefinition": [
                        {
                            "wildcard": (
                                f"*{self.sentinel_job.job_def.job_definition_name}*"
                            )
                        },
                    ],
                    "status": ["FAILED", "SUCCEEDED"],
                },
            ),
            targets=[
                events_targets.LambdaFunction(
                    handler=self.job_monitor_lambda,
                    retry_attempts=3,
                )
            ],
        )

        self._setup_phase0_shadow(settings)

        # ----------------------------------------------------------------------
        # Job requeuer
        # ----------------------------------------------------------------------
        self.job_requeuer_lambda = lambda_python.PythonFunction(
            self,
            "JobRequeuerHandler",
            entry="src/",
            index="job_requeuer/handler.py",
            handler="handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            memory_size=256,
            timeout=Duration.minutes(1),
            environment={
                "PROCESSING_BUCKET_NAME": self.processing_bucket.bucket_name,
                "BATCH_QUEUE_NAME": self.batch_infra.queue.job_queue_name,
                "BATCH_JOB_DEFINITION_NAME": (
                    self.sentinel_job.job_def.job_definition_name
                ),
                "OUTPUT_BUCKET_NAME": self.output_bucket.bucket_name,
            },
            bundling=lambda_python.BundlingOptions(
                asset_excludes=LAMBDA_EXCLUDE,
            ),
        )

        self.job_requeuer_lambda.add_to_role_policy(self.batch_submit_job_policy)
        self.processing_bucket.grant_read_write(self.job_requeuer_lambda)
        self.job_retry.queue.grant_consume_messages(self.job_requeuer_lambda)

        self.job_requeuer_lambda.add_event_source_mapping(
            "JobRequeuerRetryQueueTrigger",
            batch_size=100,
            max_batching_window=Duration.minutes(1),
            report_batch_item_failures=True,
            event_source_arn=self.job_retry.queue.queue_arn,
        )

        # ----------------------------------------------------------------------
        # Granule-init Lambda (Sentinel-2 arrival → AWAITING or SUBMITTED)
        # ----------------------------------------------------------------------
        self.granule_init_queue = sqs.Queue(
            self,
            "GranuleInitQueue",
            retention_period=Duration.days(14),
            visibility_timeout=Duration.minutes(10),
        )

        self.sentinel_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notifications.SqsDestination(self.granule_init_queue),
        )

        self.granule_init_lambda = lambda_python.PythonFunction(
            self,
            "GranuleInitLambda",
            entry="src/",
            index="granule_init/handler.py",
            handler="handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            memory_size=512,
            timeout=Duration.minutes(10),
            environment={
                "PROCESSING_BUCKET_NAME": self.processing_bucket.bucket_name,
                "SENTINEL_BUCKET_NAME": self.sentinel_bucket.bucket_name,
                "OUTPUT_BUCKET_NAME": self.output_bucket.bucket_name,
                "AUX_DATA_BUCKET_NAME": self.aux_data_bucket.bucket_name,
                "BATCH_QUEUE_NAME": self.batch_infra.queue.job_queue_name,
                "MAX_ACTIVE_JOBS": str(settings.MAX_ACTIVE_JOBS),
                "SENTINEL_JOB_DEFINITION_NAME": (
                    self.sentinel_job.job_def.job_definition_name
                ),
            },
            layers=[self.powertools_layer],
            bundling=lambda_python.BundlingOptions(
                asset_excludes=LAMBDA_EXCLUDE,
            ),
        )

        self.granule_init_queue.grant_consume_messages(self.granule_init_lambda)
        self.processing_bucket.grant_read_write(self.granule_init_lambda)
        self.sentinel_bucket.grant_read(self.granule_init_lambda)
        self.aux_data_bucket.grant_read(self.granule_init_lambda)

        self.granule_init_lambda.add_to_role_policy(self.batch_submit_job_policy)
        self.granule_init_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=["*"],
                actions=["batch:ListJobs"],
            )
        )

        self.granule_init_lambda.add_event_source_mapping(
            "GranuleInitQueueTrigger",
            batch_size=1,
            max_batching_window=Duration.seconds(0),
            report_batch_item_failures=True,
            event_source_arn=self.granule_init_queue.queue_arn,
        )

        # ----------------------------------------------------------------------
        # Ancillary-trigger Lambda (ancillary data arrives → fan-out per granule)
        # NOTE: S3 event notifications on the external aux data bucket must be
        # configured separately (the bucket is not managed by this stack).
        # ----------------------------------------------------------------------
        self.ancillary_trigger_queue = sqs.Queue(
            self,
            "AncillaryTriggerQueue",
            queue_name=settings.ANCILLARY_TRIGGER_QUEUE_NAME,
            retention_period=Duration.days(14),
            visibility_timeout=Duration.minutes(2),
            enforce_ssl=True,
            encryption=sqs.QueueEncryption.SQS_MANAGED,
        )

        # Internal queue: one message per AWAITING granule, consumed by the
        # submit Lambda. Separate from the trigger queue so it can be alarmed
        # on independently and scaled without affecting S3 event delivery.
        self.ancillary_submit = QueueWithDlq(
            self,
            "AncillarySubmitQueue",
            queue_name=settings.ANCILLARY_SUBMIT_QUEUE_NAME,
            dlq_name=settings.ANCILLARY_SUBMIT_DLQ_NAME,
            visibility_timeout=Duration.minutes(2),
            max_receive_count=3,
        )

        self.ancillary_trigger_lambda = lambda_python.PythonFunction(
            self,
            "AncillaryTriggerLambda",
            entry="src/",
            index="ancillary_trigger/handler.py",
            handler="handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            memory_size=256,
            timeout=Duration.minutes(2),
            environment={
                "PROCESSING_BUCKET_NAME": self.processing_bucket.bucket_name,
                "ANCILLARY_SUBMIT_QUEUE_URL": self.ancillary_submit.queue.queue_url,
            },
            layers=[self.powertools_layer],
            bundling=lambda_python.BundlingOptions(
                asset_excludes=LAMBDA_EXCLUDE,
            ),
        )

        self.ancillary_trigger_queue.grant_consume_messages(
            self.ancillary_trigger_lambda
        )
        self.processing_bucket.grant_read(self.ancillary_trigger_lambda)
        self.ancillary_submit.queue.grant_send_messages(self.ancillary_trigger_lambda)

        self.ancillary_trigger_lambda.add_event_source_mapping(
            "AncillaryTriggerQueueTrigger",
            batch_size=1,
            max_batching_window=Duration.seconds(0),
            report_batch_item_failures=True,
            event_source_arn=self.ancillary_trigger_queue.queue_arn,
        )

        # ----------------------------------------------------------------------
        # Ancillary-submit Lambda (per-granule Batch submission)
        # ----------------------------------------------------------------------
        self.ancillary_submit_lambda = lambda_python.PythonFunction(
            self,
            "AncillarySubmitLambda",
            entry="src/",
            index="ancillary_submit/handler.py",
            handler="handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            memory_size=256,
            timeout=Duration.minutes(2),
            environment={
                "PROCESSING_BUCKET_NAME": self.processing_bucket.bucket_name,
                "AUX_DATA_BUCKET_NAME": self.aux_data_bucket.bucket_name,
                "BATCH_QUEUE_NAME": self.batch_infra.queue.job_queue_name,
                "SENTINEL_JOB_DEFINITION_NAME": (
                    self.sentinel_job.job_def.job_definition_name
                ),
                "OUTPUT_BUCKET_NAME": self.output_bucket.bucket_name,
            },
            layers=[self.powertools_layer],
            bundling=lambda_python.BundlingOptions(
                asset_excludes=LAMBDA_EXCLUDE,
            ),
        )

        self.ancillary_submit.queue.grant_consume_messages(self.ancillary_submit_lambda)
        self.processing_bucket.grant_read_write(self.ancillary_submit_lambda)
        self.aux_data_bucket.grant_read(self.ancillary_submit_lambda)
        self.ancillary_submit_lambda.add_to_role_policy(self.batch_submit_job_policy)

        self.ancillary_submit_lambda.add_event_source_mapping(
            "AncillarySubmitQueueTrigger",
            batch_size=1,
            max_batching_window=Duration.seconds(0),
            report_batch_item_failures=True,
            event_source_arn=self.ancillary_submit.queue.queue_arn,
        )

    def _setup_phase0_shadow(self, settings: StackSettings) -> None:
        """Wire up Phase 0 shadow observability against the existing system.

        Adds an EventBridge rule that routes completed Batch jobs from the existing
        queue/job-definition to the same job_monitor Lambda, which writes shadow=True
        canonical records. Delete or disable this method once Phase 1 is fully deployed.
        """
        if not (
            settings.PHASE0_BATCH_QUEUE_ARN
            and settings.PHASE0_SENTINEL_JOB_DEFINITION_NAME
        ):
            return

        self.phase0_job_events_rule = events.Rule(
            self,
            "Phase0JobEventsRule",
            event_pattern=events.EventPattern(
                source=["aws.batch"],
                detail={
                    "jobQueue": [settings.PHASE0_BATCH_QUEUE_ARN],
                    "jobDefinition": [
                        {
                            "wildcard": (
                                f"*{settings.PHASE0_SENTINEL_JOB_DEFINITION_NAME}*"
                            )
                        }
                    ],
                    "status": ["FAILED", "SUCCEEDED"],
                },
            ),
            targets=[
                events_targets.LambdaFunction(
                    handler=self.job_monitor_lambda,
                    retry_attempts=3,
                )
            ],
        )

    def _make_bucket(self, construct_id: str, bucket_name: str) -> s3.Bucket:
        """Create a stack-managed S3 bucket with standard settings.

        Applies S3-managed encryption, SSL-only access policy, and lifecycle
        rules to expire delete markers and abort incomplete multipart uploads.
        """
        return s3.Bucket(
            self,
            construct_id,
            bucket_name=bucket_name,
            removal_policy=RemovalPolicy.DESTROY,
            enforce_ssl=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            lifecycle_rules=[
                s3.LifecycleRule(expired_object_delete_marker=True),
                s3.LifecycleRule(
                    abort_incomplete_multipart_upload_after=Duration.days(1),
                    noncurrent_version_expiration=Duration.days(1),
                ),
            ],
        )
