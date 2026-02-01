#!/usr/bin/env python3
"""
Create an EC2 instance in us-east-2 optimized for large file transfer (e.g. 1TB+).
Designed to run from AWS CloudShell. Uses 8 vCPUs, fast EBS gp3, high network.
Amazon Linux (AL2023 or AL2). After launch, connect from this CloudShell with
Session Manager: aws ssm start-session --target <instance-id> --region us-east-2
Data volume at /data. No SSH key required.
"""

import argparse
import base64
import json
import time

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-2"
INSTANCE_TYPE = "m6i.2xlarge"  # 8 vCPU, 32 GiB, up to 12.5 Gbps
ROOT_VOLUME_GB = 30
DATA_VOLUME_GB = 1500
DATA_VOLUME_IOPS = 3000
DATA_VOLUME_THROUGHPUT_MB = 500  # so disk is not the bottleneck for 1TB transfer


def get_latest_ami(ec2):
    """Return latest Amazon Linux AMI in the region (AL2023, else Amazon Linux 2)."""
    for name_filter, default_root in [
        ("al2023-ami-*-x86_64", "/dev/xvda"),
        ("amzn2-ami-hvm-*-x86_64-gp2", "/dev/xvda"),
    ]:
        try:
            amis = ec2.describe_images(
                Owners=["amazon"],
                Filters=[
                    {"Name": "name", "Values": [name_filter]},
                    {"Name": "state", "Values": ["available"]},
                    {"Name": "architecture", "Values": ["x86_64"]},
                ],
            )
            if amis["Images"]:
                latest = sorted(amis["Images"], key=lambda x: x["CreationDate"], reverse=True)[0]
                root = latest.get("RootDeviceName") or default_root
                return latest["ImageId"], root
        except Exception:
            continue
    raise RuntimeError("No Amazon Linux AMI found (tried AL2023 and AL2)")


def ensure_iam_role(iam, role_name):
    """Create or reuse IAM role with SSM + S3 for transfer. Returns (role_arn, instance_profile_name)."""
    ssm_policy = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
    instance_profile_name = f"{role_name}-profile"
    trust = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}
        ],
    }
    s3_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"],
                "Resource": "*",
            }
        ],
    }

    try:
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust),
            Description="EC2 role for file transfer (SSM + S3)",
        )
        role_arn = role["Role"]["Arn"]
        iam.attach_role_policy(RoleName=role_name, PolicyArn=ssm_policy)
        iam.put_role_policy(RoleName=role_name, PolicyName="S3Transfer", PolicyDocument=json.dumps(s3_policy))
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityAlreadyExists":
            raise
        role = iam.get_role(RoleName=role_name)
        role_arn = role["Role"]["Arn"]
        iam.put_role_policy(RoleName=role_name, PolicyName="S3Transfer", PolicyDocument=json.dumps(s3_policy))

    try:
        iam.create_instance_profile(InstanceProfileName=instance_profile_name)
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityAlreadyExists":
            raise
    try:
        iam.add_role_to_instance_profile(
            InstanceProfileName=instance_profile_name,
            RoleName=role_name,
        )
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("LimitExceeded", "EntityAlreadyExists"):
            raise

    # Wait for profile to be usable (roles attached)
    for _ in range(12):
        try:
            prof = iam.get_instance_profile(InstanceProfileName=instance_profile_name)
            if prof["InstanceProfile"].get("Roles"):
                break
        except Exception:
            pass
        time.sleep(5)
    return role_arn, instance_profile_name


def build_user_data(root_device):
    """User data: SSM agent first (Session Manager), then /data mount, Python + boto3 + AWS CLI."""
    return """#!/bin/bash
set -e
# Ensure SSM agent is installed and running for Session Manager (Amazon Linux)
dnf install -y amazon-ssm-agent 2>/dev/null || yum install -y amazon-ssm-agent 2>/dev/null || true
systemctl enable amazon-ssm-agent
systemctl start amazon-ssm-agent
# Second EBS is attached as nvme1n1 on Nitro
DATA_DEV=$(ls /dev/nvme*n* 2>/dev/null | grep -v nvme0n1 || true)
if [ -n "$DATA_DEV" ]; then
  DATA_DEV=$(echo "$DATA_DEV" | head -1)
  if ! blkid "$DATA_DEV" 2>/dev/null | grep -q xfs; then
    mkfs.xfs "$DATA_DEV"
  fi
  mkdir -p /data
  mount "$DATA_DEV" /data
  echo "$DATA_DEV /data xfs defaults 0 0" >> /etc/fstab
  echo "Mounted $DATA_DEV at /data"
fi
dnf install -y python3 python3-pip 2>/dev/null || yum install -y python3 python3-pip
pip3 install --break-system-packages boto3 2>/dev/null || pip3 install boto3
curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
dnf install -y unzip 2>/dev/null || yum install -y unzip
unzip -q -o /tmp/awscliv2.zip -d /tmp && /tmp/aws/install -b /usr/local/bin
echo "Transfer EC2 ready. Use /data for large files. Connect: aws ssm start-session --target <instance-id> --region us-east-2"
"""


def main():
    parser = argparse.ArgumentParser(
        description="Create an EC2 instance in us-east-2 optimized for file transfer (1TB+)."
    )
    parser.add_argument(
        "--role-name",
        default=None,
        help="IAM role name (default: ec2-transfer-role-<timestamp>)",
    )
    args = parser.parse_args()
    if args.role_name is None:
        args.role_name = f"ec2-transfer-role-{int(time.time())}"

    session = boto3.Session(region_name=REGION)
    ec2 = session.client("ec2")
    iam = session.client("iam")

    print(f"Region: {REGION}")
    print(f"Instance type: {INSTANCE_TYPE} (8 vCPU, 32 GiB, up to 12.5 Gbps)")
    print(f"Root: {ROOT_VOLUME_GB} GB gp3  |  Data: {DATA_VOLUME_GB} GB gp3, {DATA_VOLUME_THROUGHPUT_MB} MB/s")
    print()

    ami_id, root_device = get_latest_ami(ec2)
    print(f"AMI: {ami_id} (root: {root_device})")

    role_arn, instance_profile_name = ensure_iam_role(iam, args.role_name)
    print(f"IAM role: {args.role_name}")
    # EC2 often rejects new instance profile ARN until propagated; use Name and wait
    print("Waiting for IAM instance profile to propagate (15s)...")
    time.sleep(15)

    block = [
        {
            "DeviceName": root_device,
            "Ebs": {
                "VolumeSize": ROOT_VOLUME_GB,
                "VolumeType": "gp3",
                "Iops": 3000,
                "Throughput": 125,
                "DeleteOnTermination": True,
            },
        },
        {
            "DeviceName": "/dev/sdf",
            "Ebs": {
                "VolumeSize": DATA_VOLUME_GB,
                "VolumeType": "gp3",
                "Iops": DATA_VOLUME_IOPS,
                "Throughput": DATA_VOLUME_THROUGHPUT_MB,
                "DeleteOnTermination": True,
            },
        },
    ]

    user_data = build_user_data(root_device)
    launch_params = {
        "ImageId": ami_id,
        "InstanceType": INSTANCE_TYPE,
        "MinCount": 1,
        "MaxCount": 1,
        "BlockDeviceMappings": block,
        "UserData": base64.b64encode(user_data.encode()).decode(),
        "IamInstanceProfile": {"Name": instance_profile_name},
        "TagSpecifications": [
            {
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Name", "Value": "file-transfer-ec2"},
                    {"Key": "Purpose", "Value": "Large file transfer (S3 / local)"},
                ],
            }
        ],
    }

    print("Launching instance...")
    for launch_attempt in range(3):
        try:
            resp = ec2.run_instances(**launch_params)
            instance_id = resp["Instances"][0]["InstanceId"]
            print(f"Instance ID: {instance_id}")
            break
        except ClientError as e:
            err = e.response.get("Error", {})
            if err.get("Code") == "InvalidParameterValue" and "iamInstanceProfile" in (err.get("Message") or "").lower() and launch_attempt < 2:
                print("  Instance profile not ready yet, waiting 15s and retrying...")
                time.sleep(15)
                continue
            raise
    else:
        raise RuntimeError("Launch failed after retries")

    print("Waiting for instance to be running (usually 1–2 min)...")
    waiter = ec2.get_waiter("instance_running")
    waiter.wait(InstanceIds=[instance_id], WaiterConfig={"Delay": 5, "MaxAttempts": 60})
    print("Instance is running.")
    print("Waiting for SSM agent so you can connect (often 2–5 min after boot)...")
    ssm = session.client("ssm")
    for attempt in range(50):
        try:
            info = ssm.describe_instance_information(
                Filters=[{"Key": "InstanceIds", "Values": [instance_id]}]
            )
            if info.get("InstanceInformationList"):
                print("SSM ready — you can connect now.")
                break
        except Exception:
            pass
        if (attempt + 1) % 6 == 0:
            print(f"  Still waiting... ({(attempt + 1) * 10}s)")
        time.sleep(10)
    else:
        print("SSM not ready yet (connect in a few minutes with the command below).")

    print()
    print("From this CloudShell, connect with Session Manager:")
    print(f"  aws ssm start-session --target {instance_id} --region {REGION}")
    print()
    print("On the instance:")
    print("  /data  — {:.0f} GB gp3 volume for source/target files".format(DATA_VOLUME_GB))
    print("  Run setup_server_to_s3.py (from S3) or copy server_to_s3.py to upload to S3.")
    print()
    print("Console:")
    print(
        f"  https://{REGION}.console.aws.amazon.com/ec2/home?region={REGION}#InstanceDetails:instanceId={instance_id}"
    )


if __name__ == "__main__":
    main()
