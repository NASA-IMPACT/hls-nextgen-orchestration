from typing import Any, Literal

from aws_cdk import (
    Aws,
    Duration,
    Size,
    aws_batch as batch,
    aws_ecr as ecr,
    aws_ecs as ecs,
    aws_iam as iam,
    aws_logs as logs,
)
from constructs import Construct


def _parse_ecr_uri(uri: str) -> tuple[str, str] | None:
    """Parse a private ECR URI into (repo_arn, tag).

    Returns None for public registries (no 'dkr' in host).

    Examples
    --------
    >>> _parse_ecr_uri(
    ...     "012345678901.dkr.ecr.us-west-2.amazonaws.com/my-repo:latest"
    ... )
    ('arn:aws:ecr:us-west-2:012345678901:repository/my-repo', 'latest')
    >>> _parse_ecr_uri("public.ecr.aws/amazonlinux/amazonlinux:latest")
    None
    """
    if "dkr" not in uri:
        return None
    # Strip digest reference if present, then split tag
    base = uri.split("@")[0]
    path_part = base.split("/", 1)[-1]
    if ":" in path_part:
        base, tag = base.rsplit(":", 1)
    else:
        tag = "latest"
    host, repo_path = base.split("/", 1)
    host_parts = host.split(".")
    account_id, region = host_parts[0], host_parts[3]
    repo_arn = f"arn:aws:ecr:{region}:{account_id}:repository/{repo_path}"
    return repo_arn, tag


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

        self.role = iam.Role(
            self,
            "TaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            role_name=f"hls-processing-role-{stage}",
        )

        ecr_parsed = _parse_ecr_uri(container_ecr_uri)
        if ecr_parsed:
            repo_arn, image_tag = ecr_parsed
            ecr_repo = ecr.Repository.from_repository_arn(self, "EcrRepo", repo_arn)
            container_image: ecs.ContainerImage = (
                ecs.ContainerImage.from_ecr_repository(ecr_repo, tag=image_tag)
            )
        else:
            container_image = ecs.ContainerImage.from_registry(container_ecr_uri)

        self.job_def = batch.EcsJobDefinition(
            self,
            "JobDef",
            container=batch.EcsEc2ContainerDefinition(
                self,
                "BatchContainerDef",
                image=container_image,
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
