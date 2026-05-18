from typing import Any

from aws_cdk import (
    Duration,
    RemovalPolicy,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct


class ProcessingBucket(Construct):
    """Processing S3 bucket with state-pointer inventory and lifecycle rules.

    Attributes
    ----------
    bucket:
        The managed S3 bucket.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        bucket_name: str,
        state_inventory_prefix: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.bucket = s3.Bucket(
            self,
            "Bucket",
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

        # Use from_bucket_arn with a literal ARN (not a CDK token) so CDK does
        # not call add_to_resource_policy on the destination. When source ==
        # destination that creates a BucketPolicy ↔ Bucket circular dependency.
        # The explicit add_to_resource_policy call below provides the grant.
        inventory_dest = s3.Bucket.from_bucket_arn(
            self,
            "InventoryDest",
            f"arn:aws:s3:::{bucket_name}",
        )
        self.bucket.add_inventory(
            enabled=True,
            destination=s3.InventoryDestination(
                bucket=inventory_dest,
                prefix=state_inventory_prefix.rstrip("/"),
            ),
            inventory_id="state-pointers",
            format=s3.InventoryFormat.PARQUET,
            frequency=s3.InventoryFrequency.DAILY,
            objects_prefix="state/",
            optional_fields=["LastModifiedDate"],
        )
        self.bucket.add_lifecycle_rule(
            prefix=state_inventory_prefix,
            expiration=Duration.days(14),
        )
        self.bucket.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject"],
                resources=[self.bucket.arn_for_objects(f"{state_inventory_prefix}*")],
                principals=[iam.ServicePrincipal("s3.amazonaws.com")],
                effect=iam.Effect.ALLOW,
            )
        )
