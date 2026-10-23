from typing import Any, cast

from aws_cdk import (
    CfnOutput,
    aws_batch as batch,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_s3 as s3,
    custom_resources as cr,
)
from constructs import Construct

#: Host directory where NVMe instance store volume(s) are mounted
SCRATCH_HOST_PATH = "/scratch"
#: Host directory where the auxiliary data bucket prefix is mounted with Mountpoint
AUX_DATA_HOST_PATH = "/mnt/aux_data"
#: https://github.com/awslabs/mountpoint-s3/releases
MOUNTPOINT_S3_VERSION = "1.24.0"

#: Instance user data, run before ECS starts on the instance. Formatted in BatchInfra.
#: ECS_IMAGE_PULL_BEHAVIOR:
#: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/pull-behavior.html
USER_DATA_TEMPLATE = """\
echo "ECS_IMAGE_PULL_BEHAVIOR=prefer-cached" >> /etc/ecs/ecs.config

# Format and mount NVMe instance store volume(s) at {scratch_dir},
# striping them with RAID 0 if there are several
dnf install -y mdadm
ephemeral_disks=$(realpath -P /dev/disk/by-id/nvme*Instance_Storage* | uniq)
ephemeral_disks_count=$(echo "$ephemeral_disks" | wc -w)
scratch_device=""
if [[ "$ephemeral_disks_count" -eq "1" ]]; then
  scratch_device="$ephemeral_disks"
elif [[ "$ephemeral_disks_count" -gt "1" ]]; then
  scratch_device=/dev/md0
  mdadm --create "$scratch_device" --level 0 \\
    --raid-devices "$ephemeral_disks_count" $ephemeral_disks
fi
mkdir -p {scratch_dir}
if [[ -n "$scratch_device" ]]; then
  mkfs -t ext4 "$scratch_device"
  mount -t ext4 "$scratch_device" {scratch_dir}
fi
dd if=/dev/zero of={scratch_dir}/swapfile bs=128M count=128
chmod 600 {scratch_dir}/swapfile
mkswap {scratch_dir}/swapfile
swapon {scratch_dir}/swapfile
echo "{scratch_dir}/swapfile swap swap defaults 0 0" >> /etc/fstab

# Mount the aux data read-only at {aux_mount_dir}, caching on scratch. ECS
# starts after user data, so the mount exists before any job.
dnf install -y {mountpoint_rpm}
mkdir -p {aux_mount_dir} {mountpoint_cache}
mount-s3 {aux_bucket} {aux_mount_dir} \\
  --prefix {aux_prefix} \\
  --read-only \\
  --allow-other \\
  --cache {mountpoint_cache} \\
  --max-cache-size {aux_cache_max_mib}
"""


class BatchInfra(Construct):
    """AWS Batch compute environment and queues."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        vpc: ec2.IVpc,
        instance_classes: list[str] | None,
        max_vcpu: int,
        ami_id: str,
        aux_data_bucket: s3.IBucket,
        aux_data_prefix: str,
        aux_data_cache_max_mib: int = 10_240,
        base_name: str,
        stage: str,
        **kwargs: Any,
    ) -> None:
        """Setup an AWS Batch ComputeEnvironment and JobQueue

        Parameters
        ----------
        vpc:
            VPC in which the ComputeEnvironment will launch Instances.
        instance_classes:
            If provided, limit ComputeEnvironment to these instance types. Instance
            types can be provided by string value and will be converted into the
            appropriate `ec2.InstanceClass` enum. If not provided AWS Batch will use
            "optimal" instance classes.
        max_vcpu:
            Maximum number of CPUs in the ComputeEnvironment
        ami_id:
            AWS Batch Ec2 instance AMI identifier, OR name of SSM parameter that
            references the AMI ID prefixed by `resolve:ssm` (e.g,
            `resolve:ssm:/param-name`).
        aux_data_bucket:
            Bucket mounted read-only on each instance at ``AUX_DATA_HOST_PATH``.
        aux_data_prefix:
            Key prefix (ending in "/") of ``aux_data_bucket`` to mount.
        aux_data_cache_max_mib:
            Maximum size of the Mountpoint cache, which shares the NVMe scratch
            storage with jobs.
        base_name:
            Base name prefix for AWS resources. The ComputeEnvironment and JobQueue
            will be named ``{base_name}-{stage}``.
        stage:
            Environment or "stage" for resources used to help distinguish resources
            from this stack.
        """
        super().__init__(scope, construct_id, **kwargs)

        if instance_classes:
            ec2_instance_classes = [
                ec2.InstanceClass(instance_class) for instance_class in instance_classes
            ]
        else:
            ec2_instance_classes = None

        command_user_data = ec2.UserData.for_linux()
        command_user_data.add_commands(
            USER_DATA_TEMPLATE.format(
                scratch_dir=SCRATCH_HOST_PATH,
                mountpoint_rpm=(
                    "https://s3.amazonaws.com/mountpoint-s3-release/"
                    f"{MOUNTPOINT_S3_VERSION}/x86_64/"
                    f"mount-s3-{MOUNTPOINT_S3_VERSION}-x86_64.rpm"
                ),
                mountpoint_cache=f"{SCRATCH_HOST_PATH}/mountpoint-cache",
                aux_mount_dir=AUX_DATA_HOST_PATH,
                aux_bucket=aux_data_bucket.bucket_name,
                aux_prefix=aux_data_prefix,
                aux_cache_max_mib=aux_data_cache_max_mib,
            )
        )
        # Note: AWS Batch requires a multi-part UserData to be provided as it will
        # add its own sections
        multipart_user_data = ec2.MultipartUserData(parts_separator="==BOUNDARY==")
        multipart_user_data.add_part(
            ec2.MultipartBody.from_user_data(command_user_data)
        )

        if "resolve:ssm:" in ami_id:
            # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/create-launch-template.html#use-an-ssm-parameter-instead-of-an-ami-id
            ec2_machine_image = ec2.MachineImage.resolve_ssm_parameter_at_launch(
                ami_id.removeprefix("resolve:ssm:")
            )
        else:
            ec2_machine_image = ec2.MachineImage.lookup(name=ami_id)

        ecs_machine_image = batch.EcsMachineImage(
            image=ec2_machine_image,
            image_type=batch.EcsMachineImageType.ECS_AL2,
        )

        launch_template = ec2.LaunchTemplate(
            self,
            "LaunchTemplate",
            machine_image=ec2_machine_image,
            user_data=multipart_user_data,
        )

        # Add role for Batch to assume that allows reading from SSM
        compute_environment_service_role = iam.Role(
            self,
            "ServiceRole",
            assumed_by=iam.ServicePrincipal("batch.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSBatchServiceRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMReadOnlyAccess"
                ),
            ],
        )

        self.compute_environment = batch.ManagedEc2EcsComputeEnvironment(
            self,
            f"CE-{stage.capitalize()}",
            allocation_strategy=batch.AllocationStrategy.SPOT_CAPACITY_OPTIMIZED,
            images=[ecs_machine_image],
            launch_template=launch_template,
            instance_classes=ec2_instance_classes,
            use_optimal_instance_classes=ec2_instance_classes is None,
            spot=True,
            minv_cpus=0,
            maxv_cpus=max_vcpu,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC,
            ),
            vpc=vpc,
            terminate_on_update=False,
            service_role=compute_environment_service_role,
            # Replacing compute environment allows update of more settings,
            # but means we cannot specify the compute env name.
            replace_compute_environment=True,
        )

        # ManagedEc2EcsComputeEnvironment requires an override to track `$Latest`
        # Ref: https://github.com/aws/aws-cdk/issues/28137
        cfn_ce = cast(
            batch.CfnComputeEnvironment,
            self.compute_environment.node.find_child("Resource"),
        )
        cfn_ce.add_property_override(
            "ComputeResources.LaunchTemplate.Version",
            launch_template.latest_version_number,
        )

        # Mountpoint on the instance reads the aux data with the instance role
        instance_role = self.compute_environment.instance_role
        if instance_role is None:
            raise ValueError("Compute environment has no instance role")
        aux_data_bucket.grant_read(instance_role, f"{aux_data_prefix}*")
        # Enable ContainerInsights to help track resource usage
        self.enable_container_insights(self.compute_environment)

        self.queue = batch.JobQueue(
            self,
            "JobQueue",
            job_queue_name=f"{base_name}-{stage}",
        )
        self.queue.add_compute_environment(self.compute_environment, 1)

        CfnOutput(
            self,
            "JobQueueName",
            value=self.queue.job_queue_name,
        )

    def enable_container_insights(
        self, compute_environment: batch.IComputeEnvironment
    ) -> None:
        """Enable ContainerInsights for this managed ComputeEnvironment

        Ref: https://github.com/aws/aws-cdk/issues/21698#issuecomment-1898890043
        """
        batch_ecs_cluster = cr.AwsCustomResource(
            self,
            "BatchEcsCluster",
            policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                resources=cr.AwsCustomResourcePolicy.ANY_RESOURCE
            ),
            on_update=cr.AwsSdkCall(
                service="@aws-sdk/client-batch",
                action="DescribeComputeEnvironmentsCommand",
                parameters={
                    "computeEnvironments": [
                        compute_environment.compute_environment_arn
                    ],
                },
                physical_resource_id=cr.PhysicalResourceId.from_response(
                    "computeEnvironments.0.ecsClusterArn"
                ),
            ),
        )
        cr.AwsCustomResource(
            self,
            "EnableContainerInsights",
            policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                resources=cr.AwsCustomResourcePolicy.ANY_RESOURCE
            ),
            on_update=cr.AwsSdkCall(
                service="@aws-sdk/client-ecs",
                action="UpdateClusterCommand",
                parameters={
                    "cluster": batch_ecs_cluster.get_response_field_reference(
                        "computeEnvironments.0.ecsClusterArn"
                    ),
                    "settings": [
                        {
                            "name": "containerInsights",
                            "value": "enabled",
                        }
                    ],
                },
                physical_resource_id=cr.PhysicalResourceId.of("compute-resource-tags"),
            ),
        )
