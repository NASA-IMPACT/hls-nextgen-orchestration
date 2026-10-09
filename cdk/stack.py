from typing import Any

from aws_cdk import (
    ArnFormat,
    Aws,
    Duration,
    RemovalPolicy,
    Stack,
    aws_ec2 as ec2,
    aws_glue as glue,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_python_alpha as lambda_python,
    aws_logs as logs,
    aws_s3 as s3,
    aws_s3_notifications as s3_notifications,
    aws_sqs as sqs,
)
from batch_event_job_monitor.models import JobTypeConfig
from batch_event_job_monitor_cdk import (
    AthenaOutputsTable,
    AthenaRecordsTable,
    AthenaStateTable,
    JobMonitorFunction,
    JobResubmitFunction,
    MonitoringQueues,
    PartitionKeySpec,
    ProcessingBucket,
    job_definition_family_arn,
)
from constructs import Construct

from common.jobs import (
    ACQUISITION_DATE,
    JOB_TYPES,
    PHASE0_LANDSAT_AC,
    PHASE0_LANDSAT_TILE,
    PHASE0_SENTINEL,
    SENTINEL,
    phase0_job_type_config,
    sentinel_job_type_config,
)
from hls_constructs import BatchInfra, BatchJob, QueueWithDlq, create_granule_twin_view
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

        self.processing = ProcessingBucket(
            self,
            "ProcessingBucket",
            bucket_name=settings.PROCESSING_BUCKET_NAME,
            key_prefix=settings.PROCESSING_KEY_PREFIX,
            inventory_prefix=settings.INVENTORY_PREFIX,
            inventories=[
                (settings.STATE_INVENTORY_ID, "state/"),
                (settings.OUTPUTS_INVENTORY_ID, "outputs/"),
            ],
        )
        self.processing_bucket = self.processing.bucket

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
        self.athena_database = glue.CfnDatabase(
            self,
            "AthenaDatabase",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=settings.ATHENA_DATABASE_NAME,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=settings.ATHENA_DATABASE_NAME,
                description="Athena database for HLS NextGen orchestration.",
            ),
        )

        partition_keys = [
            PartitionKeySpec(
                name="job_type",
                glue_type="string",
                projection="enum",
                enum_values=JOB_TYPES,
            ),
            PartitionKeySpec(
                name=ACQUISITION_DATE,
                glue_type="string",
                projection="date",
                date_range=(settings.ATHENA_RECORDS_TABLE_START_DATE, "NOW"),
                date_format="yyyy-MM-dd",
                date_interval_unit="DAYS",
            ),
        ]

        self.athena_records = AthenaRecordsTable(
            self,
            "AthenaRecordsDatabase",
            database=self.athena_database,
            database_name=settings.ATHENA_DATABASE_NAME,
            records_bucket_name=settings.PROCESSING_BUCKET_NAME,
            key_prefix=self.processing.key_prefix,
            partition_keys=partition_keys,
            table_name=settings.ATHENA_RECORDS_TABLE_NAME,
        )
        self.granule_twin_view = create_granule_twin_view(
            self.athena_records,
            "TwinView",
            database=self.athena_database,
            database_name=settings.ATHENA_DATABASE_NAME,
            records_table=self.athena_records.records_table,
            records_table_name=settings.ATHENA_RECORDS_TABLE_NAME,
            view_name=settings.ATHENA_RECORDS_TWIN_VIEW_NAME,
        )
        self.athena_state = AthenaStateTable(
            self,
            "AthenaStateDatabase",
            database=self.athena_database,
            database_name=settings.ATHENA_DATABASE_NAME,
            inventory_location_s3path=self.processing.inventory_location(
                settings.STATE_INVENTORY_ID
            ),
            table_datetime_start=settings.ATHENA_STATE_TABLE_START_DATETIME,
            table_name=settings.ATHENA_STATE_TABLE_NAME,
            view_name=settings.ATHENA_STATE_VIEW_NAME,
            partition_keys=partition_keys,
        )
        self.athena_outputs = AthenaOutputsTable(
            self,
            "AthenaOutputsDatabase",
            database=self.athena_database,
            database_name=settings.ATHENA_DATABASE_NAME,
            inventory_location_s3path=self.processing.inventory_location(
                settings.OUTPUTS_INVENTORY_ID
            ),
            table_datetime_start=settings.ATHENA_OUTPUTS_TABLE_START_DATETIME,
            table_name=settings.ATHENA_OUTPUTS_TABLE_NAME,
            view_name=settings.ATHENA_OUTPUTS_VIEW_NAME,
            partition_keys=partition_keys,
        )

        # ----------------------------------------------------------------------
        # Shared metrics log group (all Batch workflows write here)
        # ----------------------------------------------------------------------
        self.metrics_log_group = logs.LogGroup(
            self,
            "MetricsLogGroup",
            log_group_name=f"/hls-orch/{settings.STAGE}/metrics",
            retention=logs.RetentionDays.THREE_MONTHS,
            removal_policy=RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE,
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
            base_name=settings.BATCH_BASE_NAME,
            stage=settings.STAGE,
        )

        # ----------------------------------------------------------------------
        # HLS processing compute jobs
        # ----------------------------------------------------------------------
        self.sentinel_ac_job = BatchJob(
            self,
            "SentinelJob",
            job_name="sentinel-ac",
            container_ecr_uri=settings.SENTINEL_CONTAINER_ECR_URI,
            vcpu=settings.SENTINEL_JOB_VCPU,
            memory_mb=settings.SENTINEL_JOB_MEMORY_MB,
            retry_attempts=settings.PROCESSING_JOB_RETRY_ATTEMPTS,
            metrics_log_group=self.metrics_log_group,
            environment={},
            secrets={},
            stage=settings.STAGE,
        )

        self.output_bucket.grant_read_write(self.sentinel_ac_job.role)
        self.sentinel_bucket.grant_read(self.sentinel_ac_job.role)
        self.aux_data_bucket.grant_read(self.sentinel_ac_job.role)
        if self.debug_bucket is not None:
            self.debug_bucket.grant_read(self.sentinel_ac_job.role)

        # Shared policy for Batch job submission
        self.batch_submit_job_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            resources=[
                self.batch_infra.queue.job_queue_arn,
                self.sentinel_ac_job.job_def_arn_without_revision,
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
        sentinel_config = sentinel_job_type_config(
            job_queue_arn=self.batch_infra.queue.job_queue_arn,
            job_definition_arn=job_definition_family_arn(self.sentinel_ac_job.job_def),
            max_attempts=settings.JOB_RETRY_MAX_ATTEMPTS,
        )

        self.monitoring_queues = MonitoringQueues(self, "MonitoringQueues")

        self.job_monitor = JobMonitorFunction(
            self,
            "JobMonitor",
            processing_bucket=self.processing_bucket,
            key_prefix=self.processing.key_prefix,
            job_type_configs={
                SENTINEL: sentinel_config,
                **self._phase0_job_type_configs(settings),
            },
            queues=self.monitoring_queues,
            entry="src/",
            index="job_monitor/handler.py",
            bundling=lambda_python.BundlingOptions(asset_excludes=LAMBDA_EXCLUDE),
        )

        # Only this system's own jobs are resubmitted; the existing system
        # retries its own.
        self.job_resubmit = JobResubmitFunction(
            self,
            "JobResubmit",
            job_type_configs={SENTINEL: sentinel_config},
            retry_queue=self.monitoring_queues.retry_queue,
            entry="src/",
            index="job_resubmit/handler.py",
            environment={"OUTPUT_BUCKET_NAME": self.output_bucket.bucket_name},
            bundling=lambda_python.BundlingOptions(asset_excludes=LAMBDA_EXCLUDE),
        )

        # ----------------------------------------------------------------------
        # Granule-init Lambda (Sentinel-2 arrival → AWAITING or SUBMITTED)
        # ----------------------------------------------------------------------
        self.granule_init = QueueWithDlq(
            self,
            "GranuleInitQueue",
            queue_name=settings.GRANULE_INIT_QUEUE_NAME,
            dlq_name=settings.GRANULE_INIT_DLQ_NAME,
            visibility_timeout=Duration.minutes(10),
            max_receive_count=3,
        )

        self.sentinel_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notifications.SqsDestination(self.granule_init.queue),
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
                "PROCESSING_KEY_PREFIX": self.processing.key_prefix,
                "SENTINEL_BUCKET_NAME": self.sentinel_bucket.bucket_name,
                "OUTPUT_BUCKET_NAME": self.output_bucket.bucket_name,
                "AUX_DATA_BUCKET_NAME": self.aux_data_bucket.bucket_name,
                "BATCH_QUEUE_NAME": self.batch_infra.queue.job_queue_name,
                "MAX_ACTIVE_JOBS": str(settings.MAX_ACTIVE_JOBS),
                "SENTINEL_JOB_DEFINITION_NAME": (
                    self.sentinel_ac_job.job_def.job_definition_name
                ),
            },
            layers=[self.powertools_layer],
            bundling=lambda_python.BundlingOptions(
                asset_excludes=LAMBDA_EXCLUDE,
            ),
        )

        self.granule_init.queue.grant_consume_messages(self.granule_init_lambda)
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
            event_source_arn=self.granule_init.queue.queue_arn,
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
                "PROCESSING_KEY_PREFIX": self.processing.key_prefix,
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
                "PROCESSING_KEY_PREFIX": self.processing.key_prefix,
                "AUX_DATA_BUCKET_NAME": self.aux_data_bucket.bucket_name,
                "BATCH_QUEUE_NAME": self.batch_infra.queue.job_queue_name,
                "SENTINEL_JOB_DEFINITION_NAME": (
                    self.sentinel_ac_job.job_def.job_definition_name
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

    def _phase0_job_type_configs(
        self, settings: StackSettings
    ) -> dict[str, JobTypeConfig]:
        """Shadow-monitoring configs for the existing system's job types.

        Empty unless the Phase 0 settings are set. Remove once Phase 1 is fully
        deployed.
        """
        phase0 = {
            PHASE0_SENTINEL: (
                settings.PHASE0_SENTINEL_BATCH_QUEUE_ARN,
                settings.PHASE0_SENTINEL_JOB_DEFINITION_NAME,
            ),
            PHASE0_LANDSAT_AC: (
                settings.PHASE0_LANDSAT_AC_BATCH_QUEUE_ARN,
                settings.PHASE0_LANDSAT_AC_JOB_DEFINITION_NAME,
            ),
            PHASE0_LANDSAT_TILE: (
                settings.PHASE0_LANDSAT_TILE_BATCH_QUEUE_ARN,
                settings.PHASE0_LANDSAT_TILE_JOB_DEFINITION_NAME,
            ),
        }
        return {
            job_type: phase0_job_type_config(
                job_queue_arn=job_queue_arn,
                job_definition_arn=self.format_arn(
                    service="batch",
                    resource="job-definition",
                    resource_name=job_definition_name,
                    arn_format=ArnFormat.SLASH_RESOURCE_NAME,
                ),
            )
            for job_type, (job_queue_arn, job_definition_name) in phase0.items()
            if job_queue_arn and job_definition_name
        }

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
