#!/usr/bin/env python3
"""
Push AWS profiles from CloudShell to the transfer EC2 with one command.
No copy-paste: run in CloudShell and profiles are applied on the instance.

  python3 copy_aws_profiles.py [instance-id]

If you omit instance-id, uses the running instance named "file-transfer-ec2".
Uses S3 (temp object) + SSM run-command; temp object is deleted after.
"""

import argparse
import base64
import json
import os
import sys
import time
import uuid

AWS_DIR = os.path.expanduser("~/.aws")
CREDENTIALS_PATH = os.path.join(AWS_DIR, "credentials")
CONFIG_PATH = os.path.join(AWS_DIR, "config")
REGION = "us-east-2"


def get_profile_data():
    """Read credentials and config; return dict with base64 payload."""
    data = {}
    if os.path.isfile(CREDENTIALS_PATH):
        with open(CREDENTIALS_PATH, "r") as f:
            data["credentials"] = f.read()
    else:
        data["credentials"] = ""
    if os.path.isfile(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            data["config"] = f.read()
    else:
        data["config"] = ""

    if not data["credentials"] and not data["config"]:
        print("No ~/.aws/credentials or ~/.aws/config found.", file=sys.stderr)
        sys.exit(1)
    return data


def find_transfer_instance(ec2):
    """Return instance ID of running instance with Name=file-transfer-ec2."""
    r = ec2.describe_instances(
        Filters=[
            {"Name": "instance-state-name", "Values": ["running"]},
            {"Name": "tag:Name", "Values": ["file-transfer-ec2"]},
        ]
    )
    for res in r.get("Reservations", []):
        for inst in res.get("Instances", []):
            return inst["InstanceId"]
    return None


def push_profiles(instance_id=None):
    import boto3
    from botocore.exceptions import ClientError

    data = get_profile_data()
    payload_b64 = base64.b64encode(json.dumps(data).encode()).decode()

    session = boto3.Session(region_name=REGION)
    sts = session.client("sts")
    account_id = sts.get_caller_identity()["Account"]
    bucket = f"ec2-scripts-{account_id}"
    key = f"temp-aws-profiles/{uuid.uuid4().hex}.bin"

    s3 = session.client("s3")
    ec2_client = session.client("ec2")
    ssm = session.client("ssm")

    if not instance_id:
        instance_id = find_transfer_instance(ec2_client)
        if not instance_id:
            print("No running instance with Name=file-transfer-ec2. Pass instance-id.", file=sys.stderr)
            sys.exit(1)
        print(f"Using instance: {instance_id}")

    # Ensure bucket exists
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            if REGION == "us-east-1":
                s3.create_bucket(Bucket=bucket)
            else:
                s3.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={"LocationConstraint": REGION},
                )
        else:
            raise

    print(f"Uploading profiles to s3://{bucket}/{key}...")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload_b64.encode(),
        ContentType="application/octet-stream",
    )

    # SSM script: download, decode, write ~/.aws/ for root, ssm-user, ec2-user
    # Run Command runs as root; chown so ssm-user/ec2-user can read their copy
    script = f"""set -e
aws s3 cp s3://{bucket}/{key} /tmp/aws_profiles.bin --region {REGION}
python3 << 'PYEOF'
import base64, json, os
d = json.loads(base64.b64decode(open("/tmp/aws_profiles.bin").read()).decode())
for home in ["/root", "/home/ssm-user", "/home/ec2-user"]:
    if not os.path.isdir(home):
        continue
    aws_dir = os.path.join(home, ".aws")
    os.makedirs(aws_dir, mode=0o700, exist_ok=True)
    if d.get("credentials"):
        p = os.path.join(aws_dir, "credentials")
        open(p, "w").write(d["credentials"])
        os.chmod(p, 0o600)
    if d.get("config"):
        p = os.path.join(aws_dir, "config")
        open(p, "w").write(d["config"])
        os.chmod(p, 0o600)
    print("Profiles written to " + aws_dir)
PYEOF
chown -R ssm-user:ssm-user /home/ssm-user/.aws 2>/dev/null || true
chown -R ec2-user:ec2-user /home/ec2-user/.aws 2>/dev/null || true
rm -f /tmp/aws_profiles.bin
"""

    print("Running SSM command on instance...")
    cmd = ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [script]},
    )
    command_id = cmd["Command"]["CommandId"]

    # Invocation can take a few seconds to appear; retry on InvocationDoesNotExist
    out = None
    status = None
    for attempt in range(90):
        try:
            out = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance_id)
            status = out.get("Status")
            if status in ("Success", "Failed", "Cancelled"):
                break
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "InvocationDoesNotExist":
                if attempt < 89:
                    time.sleep(2)
                    continue
                raise
            raise
        time.sleep(2)
    else:
        print("Command timed out; profiles may still be applied. Check SSM in console.", file=sys.stderr)

    # Delete temp object
    try:
        s3.delete_object(Bucket=bucket, Key=key)
    except Exception:
        pass

    if status == "Success" and out:
        print("Done. On the instance, run: aws sts get-caller-identity")
        if out.get("StandardErrorContent"):
            print(out["StandardErrorContent"], file=sys.stderr)
        if out.get("StandardOutputContent"):
            print(out["StandardOutputContent"])
    else:
        if status:
            print(f"Command status: {status}", file=sys.stderr)
        if out and out.get("StandardErrorContent"):
            print(out["StandardErrorContent"], file=sys.stderr)
        if status != "Success":
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Push AWS profiles from CloudShell to the transfer EC2 (one command, no paste)."
    )
    parser.add_argument(
        "instance_id",
        nargs="?",
        default=None,
        help="EC2 instance ID (default: find running file-transfer-ec2)",
    )
    args = parser.parse_args()
    push_profiles(args.instance_id)


if __name__ == "__main__":
    main()
