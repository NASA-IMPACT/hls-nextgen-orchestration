from typing import Any, Dict

from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_batch as batch,
    aws_ec2 as ec2,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_python_alpha as lambda_python,
    aws_s3 as s3,
    aws_s3_notifications as s3_notifications,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subscriptions,
    aws_sqs as sqs,
)
from constructs import Construct
from hls_constructs import BatchInfra, BatchJob
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
            # Apply IAM permission boundary to entire stack
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

        # Use S3 VPC endpoint to accelerate within-region traffic.
        # For now, only add to prod stack to avoid unnecessary duplicates.
        # It'd be better to have this as part of our (MCP owned) networking stack
        # in the long run.
        if settings.STAGE == "prod":
            self.vpc.add_gateway_endpoint(
                "S3",
                service=ec2.GatewayVpcEndpointAwsService.S3,
            )

        # ----------------------------------------------------------------------
        # Buckets
        # ----------------------------------------------------------------------
        self.aux_data_bucket = s3.Bucket.from_bucket_name(
            self,
            "AuxDataBucket",
            bucket_name=settings.AUX_DATA_BUCKET_NAME,
        )

        self.fmask_bucket = s3.Bucket(
            self,
            "FmaskBucket",
            bucket_name=settings.FMASK_OUTPUT_BUCKET_NAME,
            removal_policy=RemovalPolicy.DESTROY,
            lifecycle_rules=[
                s3.LifecycleRule(expired_object_delete_marker=True),
                s3.LifecycleRule(
                    abort_incomplete_multipart_upload_after=Duration.days(1),
                    noncurrent_version_expiration=Duration.days(1),
                ),
            ],
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        self.processing_bucket = s3.Bucket(
            self,
            "ProcessingBucket",
            bucket_name=settings.PROCESSING_BUCKET_NAME,
            removal_policy=RemovalPolicy.DESTROY,
            lifecycle_rules=[
                # Setting expired_object_delete_marker cannot be done within a
                # lifecycle rule that also specifies expiration, expiration_date, or
                # tag_filters.
                s3.LifecycleRule(expired_object_delete_marker=True),
                s3.LifecycleRule(
                    abort_incomplete_multipart_upload_after=Duration.days(1),
                    noncurrent_version_expiration=Duration.days(1),
                ),
            ],
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        self.sentinel_bucket = s3.Bucket(
            self,
            "SentinelBucket",
            bucket_name=settings.SENTINEL_BUCKET_NAME,
            removal_policy=RemovalPolicy.DESTROY,
            lifecycle_rules=[
                # Setting expired_object_delete_marker cannot be done within a
                # lifecycle rule that also specifies expiration, expiration_date, or
                # tag_filters.
                s3.LifecycleRule(expired_object_delete_marker=True),
                s3.LifecycleRule(
                    abort_incomplete_multipart_upload_after=Duration.days(1),
                    noncurrent_version_expiration=Duration.days(1),
                ),
            ],
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        bucket_envvars = {
            "FMASK_OUTPUT_BUCKET_NAME": settings.FMASK_OUTPUT_BUCKET_NAME,
        }

        self.debug_bucket: s3.IBucket | None
        if settings.DEBUG_BUCKET_NAME:
            self.debug_bucket = s3.Bucket.from_bucket_name(
                self,
                "DebugBucket",
                bucket_name=settings.DEBUG_BUCKET_NAME,
            )
            bucket_envvars["DEBUG_BUCKET"] = settings.DEBUG_BUCKET_NAME
        else:
            self.debug_bucket = None

        # ----------------------------------------------------------------------
        # S3 processing bucket inventory to track our logs
        # ----------------------------------------------------------------------
        inventory_id = "granule_processing_logs"
        self.processing_bucket.add_inventory(
            enabled=True,
            destination=s3.InventoryDestination(
                bucket=s3.Bucket.from_bucket_name(
                    self, "ProcessingBucketRef", settings.PROCESSING_BUCKET_NAME
                ),
                prefix=settings.PROCESSING_BUCKET_LOGS_INVENTORY_PREFIX.rstrip("/"),
            ),
            inventory_id=inventory_id,
            format=s3.InventoryFormat.PARQUET,
            frequency=s3.InventoryFrequency.DAILY,
            objects_prefix=settings.PROCESSING_BUCKET_LOG_PREFIX,
            optional_fields=["LastModifiedDate"],
        )
        self.processing_bucket.add_lifecycle_rule(
            prefix=settings.PROCESSING_BUCKET_LOGS_INVENTORY_PREFIX,
            expiration=Duration.days(14),
        )

        # S3 service also needs permissions to push to the bucket
        self.processing_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:PutObject",
                ],
                resources=[
                    self.processing_bucket.arn_for_objects(
                        f"{settings.PROCESSING_BUCKET_LOGS_INVENTORY_PREFIX}*"
                    ),
                ],
                principals=[
                    iam.ServicePrincipal("s3.amazonaws.com"),
                ],
                effect=iam.Effect.ALLOW,
            )
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
        secrets: Dict[str, batch.Secret] = {}

        self.fmask_job = BatchJob(
            self,
            "FmaskJob",
            container_ecr_uri=settings.FMASK_CONTAINER_ECR_URI,
            vcpu=settings.FMASK_JOB_VCPU,
            memory_mb=settings.FMASK_JOB_MEMORY_MB,
            retry_attempts=settings.PROCESSING_JOB_RETRY_ATTEMPTS,
            log_group_name=settings.PROCESSING_LOG_GROUP_NAME,
            environment={
                "PYTHONUNBUFFERED": "TRUE",
                "SENTINEL_BUCKET_NAME": self.sentinel_bucket.bucket_name,
                "FMASK_OUTPUT_BUCKET_NAME": self.fmask_bucket.bucket_name,
                "AUX_DATA_BUCKET_NAME": self.aux_data_bucket.bucket_name,
            },
            secrets=secrets,
            stage=settings.STAGE,
        )

        self.fmask_bucket.grant_read_write(self.fmask_job.role)
        self.sentinel_bucket.grant_read(self.fmask_job.role)
        self.aux_data_bucket.grant_read(self.fmask_job.role)
        if self.debug_bucket is not None:
            self.debug_bucket.grant_read(self.fmask_job.role)

        # ----------------------------------------------------------------------
        # Job monitor & retry system
        # ----------------------------------------------------------------------
        # Queue for job failures we need to investigate (bugs, repeat errors, etc)
        self.job_failure_dlq = sqs.Queue(
            self,
            "JobRetryFailureDLQ",
            queue_name=settings.JOB_FAILURE_DLQ_NAME,
            retention_period=Duration.days(14),
            enforce_ssl=True,
            encryption=sqs.QueueEncryption.SQS_MANAGED,
        )

        # Queue for failed AWS Batch processing jobs we want to requeue
        self.job_retry_queue = sqs.Queue(
            self,
            "JobRetryFailureQueue",
            queue_name=settings.JOB_RETRY_QUEUE_NAME,
            dead_letter_queue=sqs.DeadLetterQueue(
                queue=self.job_failure_dlq,
                # Route to DLQ immediately if we can't process
                max_receive_count=1,
            ),
            retention_period=Duration.days(14),
            visibility_timeout=Duration.minutes(2),
            enforce_ssl=True,
            encryption=sqs.QueueEncryption.SQS_MANAGED,
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
                "PROCESSING_BUCKET_LOG_PREFIX": settings.PROCESSING_BUCKET_LOG_PREFIX,
                "BATCH_QUEUE_NAME": self.batch_infra.queue.job_queue_name,
                "JOB_RETRY_QUEUE_URL": self.job_retry_queue.queue_url,
                "JOB_FAILURE_DLQ_URL": self.job_failure_dlq.queue_url,
                "PROCESSING_JOB_RETRY_ATTEMPTS": str(
                    settings.PROCESSING_JOB_RETRY_ATTEMPTS
                ),
            },
            bundling=lambda_python.BundlingOptions(
                asset_excludes=LAMBDA_EXCLUDE,
            ),
        )
        self.processing_bucket.grant_read_write(
            self.job_monitor_lambda,
        )
        self.job_retry_queue.grant_send_messages(self.job_monitor_lambda)
        self.job_failure_dlq.grant_send_messages(self.job_monitor_lambda)

        # Events from AWS Batch "job state change events" in our processing queue
        # Ref: https://docs.aws.amazon.com/batch/latest/userguide/batch_job_events.html
        self.processing_job_events_rule = events.Rule(
            self,
            "ProcessingJobEventsRule",
            event_pattern=events.EventPattern(
                source=["aws.batch"],
                detail={
                    # only retry jobs from our queue and job definition that failed
                    # on their last attempt
                    "jobQueue": [self.batch_infra.queue.job_queue_arn],
                    "jobDefinition": [
                        {
                            "wildcard": (
                                f"*{self.fmask_job.job_def.job_definition_name}*"
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

        # ----------------------------------------------------------------------
        # Requeuer
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
                "BATCH_QUEUE_NAME": self.batch_infra.queue.job_queue_name,
                "BATCH_JOB_DEFINITION_NAME": (
                    self.fmask_job.job_def.job_definition_name
                ),
                **bucket_envvars,
            },
            bundling=lambda_python.BundlingOptions(
                asset_excludes=LAMBDA_EXCLUDE,
            ),
        )

        self.batch_submit_job_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            resources=[
                self.batch_infra.queue.job_queue_arn,
                self.fmask_job.job_def_arn_without_revision,
            ],
            actions=[
                "batch:SubmitJob",
            ],
        )
        self.job_requeuer_lambda.add_to_role_policy(self.batch_submit_job_policy)
        self.job_retry_queue.grant_consume_messages(self.job_requeuer_lambda)

        # Requeuer consumes from queue that the "job monitor" publishes to
        self.job_requeuer_lambda.add_event_source_mapping(
            "JobRequeuerRetryQueueTrigger",
            batch_size=100,
            max_batching_window=Duration.minutes(1),
            report_batch_item_failures=True,
            event_source_arn=self.job_retry_queue.queue_arn,
        )

        self.sentinel_topic = sns.Topic(
            self,
            "SentinelTopic",
        )

        self.sentinel_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notifications.SnsDestination(self.sentinel_topic),
        )

        self.granule_entry_queue = sqs.Queue(
            self,
            "GranuleEntryQueue",
            retention_period=Duration.days(14),
            visibility_timeout=Duration.minutes(10),
        )

        self.sentinel_topic.add_subscription(
            sns_subscriptions.SqsSubscription(self.granule_entry_queue)
        )

        self.powertools_layer = lambda_.LayerVersion.from_layer_version_arn(
            self,
            "PowertoolsLayer",
            layer_version_arn=f"arn:aws:lambda:{self.region}:017000801446:layer:AWSLambdaPowertoolsPythonV3-python312-x86_64:18",
        )

        self.granule_entry_lambda = lambda_python.PythonFunction(
            self,
            "GranuleEntryLambda",
            entry="src/",
            index="granule_entry/handler.py",
            handler="handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            memory_size=512,
            timeout=Duration.minutes(10),
            environment={
                "PROCESSING_BUCKET_NAME": self.processing_bucket.bucket_name,
                "PROCESSING_BUCKET_LOG_PREFIX": settings.PROCESSING_BUCKET_LOG_PREFIX,
                "FMASK_OUTPUT_BUCKET_NAME": self.fmask_bucket.bucket_name,
                "AUX_DATA_BUCKET_NAME": self.aux_data_bucket.bucket_name,
                "BATCH_QUEUE_NAME": self.batch_infra.queue.job_queue_name,
                "MAX_ACTIVE_JOBS": str(settings.MAX_ACTIVE_JOBS),
                "FMASK_JOB_DEFINITION_NAME": self.fmask_job.job_def.job_definition_name,
            },
            layers=[self.powertools_layer],
            bundling=lambda_python.BundlingOptions(
                asset_excludes=LAMBDA_EXCLUDE,
            ),
        )

        self.granule_entry_queue.grant_consume_messages(self.granule_entry_lambda)
        self.processing_bucket.grant_read_write(self.granule_entry_lambda)
        self.aux_data_bucket.grant_read_write(self.granule_entry_lambda)

        self.granule_entry_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=[
                    self.batch_infra.queue.job_queue_arn,
                    self.fmask_job.job_def_arn_without_revision,
                ],
                actions=[
                    "batch:SubmitJob",
                ],
            )
        )
        self.granule_entry_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=["*"],
                actions=[
                    "batch:ListJobs",
                ],
            )
        )

        # Granule queuer consumes from the granule queue
        self.granule_entry_lambda.add_event_source_mapping(
            "GranuleQueuerQueueTrigger",
            batch_size=100,
            max_batching_window=Duration.minutes(1),
            report_batch_item_failures=True,
            event_source_arn=self.granule_entry_queue.queue_arn,
        )
