from typing import Any, Literal

from aws_cdk import (
    Aws,
    Duration,
    Size,
    aws_batch as batch,
    aws_ecs as ecs,
    aws_iam as iam,
    aws_logs as logs,
)
from constructs import Construct


def ecr_uri_to_repo_arn(uri: str) -> str | None:
    """Convert an ECR container URI to the ARN for the repository

    This returns "None" if the container URI is not in ECR (i.e., it's public)
    since that has no ARN.

    Examples
    --------
    >>> ecr_uri_to_repo_arn(
    ...     "012345678901.dkr.ecr.us-west-2.amazonaws.com/my-repo:latest"
    ... )
    arn:aws:ecr:us-west-2:012345678901:repository/my-repo
    >>> ecr_uri_to_repo_arn("public.ecr.aws/amazonlinux/amazonlinux:latest")
    None
    """
    if "dkr" not in uri:
        return None

    tagless = uri.split(":")[0]
    dkr, repo = tagless.split("/")
    account_id, _, _, region, _, _ = dkr.split(".")
    return f"arn:aws:ecr:{region}:{account_id}:repository/{repo}"


class BatchJob(Construct):
    """An AWS Batch job running a Docker container"""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        container_ecr_uri: str,
        vcpu: int,
        memory_mb: int,
        retry_attempts: int,
        log_group_name: str,
        environment: None | dict[str, str] = None,
        secrets: None | dict[str, batch.Secret] = None,
        stage: Literal["dev", "prod"],
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.log_group = logs.LogGroup(
            self,
            "JobLogGroup",
            log_group_name=log_group_name,
        )

        # Execution role needs ECR permissions to pull from private repo
        # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html#ecr-required-iam-permissions
        execution_role = iam.Role(
            self,
            "ExecutionRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                )
            ],
        )
        execution_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=["*"],
                actions=[
                    "ecr:GetAuthorizationToken",
                ],
            )
        )
        if ecr_repo_arn := ecr_uri_to_repo_arn(container_ecr_uri):
            execution_role.add_to_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    resources=[
                        ecr_repo_arn,
                    ],
                    actions=[
                        "ecr:BatchGetImage",
                        "ecr:GetDownloadUrlForLayer",
                    ],
                )
            )

        self.role = iam.Role(
            self,
            "TaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            role_name=f"hls-processing-role-{stage}",
        )

        self.job_def = batch.EcsJobDefinition(
            self,
            "JobDef",
            container=batch.EcsEc2ContainerDefinition(
                self,
                "BatchContainerDef",
                image=ecs.ContainerImage.from_registry(container_ecr_uri),
                execution_role=execution_role,
                job_role=self.role,
                cpu=vcpu,
                memory=Size.mebibytes(memory_mb),
                logging=ecs.LogDriver.aws_logs(
                    stream_prefix="job",
                    log_group=self.log_group,
                ),
                secrets=secrets,
                environment=environment or {},
            ),
            timeout=Duration.hours(1),
            retry_attempts=retry_attempts,
            retry_strategies=[
                batch.RetryStrategy.of(
                    batch.Action.RETRY, batch.Reason.CANNOT_PULL_CONTAINER
                ),
                batch.RetryStrategy.of(
                    batch.Action.RETRY, batch.Reason.SPOT_INSTANCE_RECLAIMED
                ),
                batch.RetryStrategy.of(
                    batch.Action.EXIT,
                    batch.Reason.custom(on_reason="*"),
                ),
            ],
            propagate_tags=True,
        )

        # It's useful to have the ARN of the job definition _without_ the revision
        # so submitted jobs use the "latest" active job
        self.job_def_arn_without_revision = ":".join(
            [
                "arn",
                "aws",
                "batch",
                Aws.REGION,
                Aws.ACCOUNT_ID,
                f"job-definition/{self.job_def.job_definition_name}",
            ]
        )
