from typing import Any

from aws_cdk import (
    Aws,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3files as s3files,
)
from constructs import Construct

#: NFS port used by S3 Files mount targets
NFS_PORT = 2049


class S3FileSystem(Construct):
    """An S3 Files file system exposing a bucket prefix over NFS within a VPC."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        bucket: s3.IBucket,
        prefix: str,
        vpc: ec2.IVpc,
        subnets: list[ec2.ISubnet],
        clients: list[ec2.IConnectable],
        **kwargs: Any,
    ) -> None:
        """Setup an S3 Files file system and its mount targets

        Parameters
        ----------
        bucket:
            Bucket backing the file system. S3 Files requires versioning enabled.
        prefix:
            Key prefix (ending in "/") the file system is scoped to.
        vpc:
            VPC to create mount targets in.
        subnets:
            Subnets to reach the file system from. Mount targets are created in
            one subnet per AZ, as S3 Files allows only one per AZ.
        clients:
            Resources (e.g., Batch compute environments) allowed to reach the
            mount targets over NFS.
        """
        super().__init__(scope, construct_id, **kwargs)

        self.bucket = bucket
        self.prefix = prefix

        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-prereq-policies.html
        role = iam.Role(
            self,
            "Role",
            assumed_by=iam.ServicePrincipal(
                "elasticfilesystem.amazonaws.com",
                conditions={
                    "StringEquals": {"aws:SourceAccount": Aws.ACCOUNT_ID},
                    "ArnLike": {
                        "aws:SourceArn": (
                            f"arn:{Aws.PARTITION}:s3files:{Aws.REGION}:"
                            f"{Aws.ACCOUNT_ID}:file-system/*"
                        )
                    },
                },
            ),
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket", "s3:ListBucketVersions"],
                resources=[bucket.bucket_arn],
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:AbortMultipartUpload",
                    "s3:DeleteObject*",
                    "s3:GetObject*",
                    "s3:List*",
                    "s3:PutObject*",
                ],
                resources=[bucket.arn_for_objects("*")],
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "events:DeleteRule",
                    "events:DisableRule",
                    "events:EnableRule",
                    "events:PutRule",
                    "events:PutTargets",
                    "events:RemoveTargets",
                ],
                resources=[
                    f"arn:{Aws.PARTITION}:events:*:*:rule/DO-NOT-DELETE-S3-Files*"
                ],
                conditions={
                    "StringEquals": {
                        "events:ManagedBy": "elasticfilesystem.amazonaws.com"
                    }
                },
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "events:DescribeRule",
                    "events:ListRuleNamesByTarget",
                    "events:ListRules",
                    "events:ListTargetsByRule",
                ],
                resources=[f"arn:{Aws.PARTITION}:events:*:*:rule/*"],
            )
        )

        self.file_system = s3files.CfnFileSystem(
            self,
            "FileSystem",
            bucket=bucket.bucket_arn,
            prefix=prefix,
            role_arn=role.role_arn,
        )
        self.file_system.node.add_dependency(role)
        self.file_system_arn = self.file_system.attr_file_system_arn

        self.security_group = ec2.SecurityGroup(
            self,
            "MountTargetSecurityGroup",
            vpc=vpc,
            description="S3 Files mount targets",
            allow_all_outbound=False,
        )
        for client in clients:
            self.security_group.connections.allow_from(
                client, ec2.Port.tcp(NFS_PORT), "NFS from S3 Files client"
            )

        subnets_by_az = {subnet.availability_zone: subnet for subnet in subnets}
        self.mount_targets = [
            s3files.CfnMountTarget(
                self,
                f"MountTarget{i}",
                file_system_id=self.file_system.attr_file_system_id,
                subnet_id=subnet.subnet_id,
                security_groups=[self.security_group.security_group_id],
            )
            for i, subnet in enumerate(subnets_by_az.values())
        ]

    def grant_read_mount(self, grantee: iam.IGrantable) -> None:
        """Allow mounting this file system read-only"""
        grantee.grant_principal.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["s3files:ClientMount"],
                resources=[self.file_system_arn],
            )
        )
        # Large reads are served directly from S3
        self.bucket.grant_read(grantee, f"{self.prefix}*")
