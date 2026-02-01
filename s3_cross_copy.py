#!/usr/bin/env python3
"""
Copy S3 objects from one bucket (with one AWS profile) to another (with another profile).
Source and destination are hardcoded below. Streams through this process—suitable for
very large objects (e.g. 1TB). Preserves folder structure. No silent failures; progress
printed frequently.

Unattended: nohup python3 s3_cross_copy.py --log-file s3_copy.log >> s3_copy.log 2>&1 &
"""

import argparse
import sys
import time
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

# --- Hardcoded source and destination (edit these) ---
SOURCE_S3_URI = "s3://kindred-datawarehouse-samples/tgcf/MX/DB3/"
SOURCE_PROFILE = "kindred"
DEST_S3_URI = "s3://kindred-0/MX2/"
DEST_PROFILE = "default"
REGION = "us-east-2"
# ----------------------------------------------------

# Multipart: S3 allows max 10,000 parts, min part 5MB (except last). For 1TB use 100MB parts.
DEFAULT_PART_BYTES = 100 * 1024 * 1024  # 100 MB
PROGRESS_INTERVAL_SEC = 2   # Print progress at least this often
HEARTBEAT_SEC = 30          # If no part in this long, print "still copying..." so user knows it's not hung


def parse_s3_url(url):
    """Return (bucket, key). Key may be prefix (trailing /)."""
    url = url.strip().replace("s3://", "", 1)
    if "/" not in url:
        return url, ""
    idx = url.index("/")
    return url[:idx], url[idx + 1 :].lstrip("/")


def list_all_objects(s3, bucket, prefix):
    """List all object keys under prefix (paginated)."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            keys.append(obj["Key"])
    return keys


def copy_one_object(
    source_s3,
    source_bucket,
    source_key,
    dest_s3,
    dest_bucket,
    dest_key,
    part_bytes,
    log_file,
    file_index=None,
    total_files=None,
):
    """
    Stream copy one object: GetObject from source in chunks, multipart upload to dest.
    Verifies size at end. Raises on error. Prints progress frequently.
    """
    # Get source size
    try:
        head = source_s3.head_object(Bucket=source_bucket, Key=source_key)
        total_size = head["ContentLength"]
    except ClientError as e:
        raise RuntimeError(f"Source head_object failed: {e}") from e

    def log(msg):
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {msg}"
        print(line, flush=True)
        if log_file:
            log_file.write(line + "\n")
            log_file.flush()

    start_time = time.time()
    prefix = f"  [{file_index}/{total_files}] " if file_index is not None else "  "
    log(f"{prefix}Copying s3://{source_bucket}/{source_key} -> s3://{dest_bucket}/{dest_key} ({total_size:,} bytes)")

    # Initiate multipart upload
    try:
        mpu = dest_s3.create_multipart_upload(Bucket=dest_bucket, Key=dest_key)
        upload_id = mpu["UploadId"]
    except ClientError as e:
        raise RuntimeError(f"create_multipart_upload failed: {e}") from e

    parts = []
    bytes_done = 0
    last_progress_log = time.time()
    last_heartbeat = time.time()
    part_num = 1

    try:
        get_stream = source_s3.get_object(Bucket=source_bucket, Key=source_key)
        body = get_stream["Body"]

        while True:
            chunk = body.read(part_bytes)
            if not chunk:
                break

            try:
                resp = dest_s3.upload_part(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    UploadId=upload_id,
                    PartNumber=part_num,
                    Body=chunk,
                )
                parts.append({"PartNumber": part_num, "ETag": resp["ETag"]})
            except ClientError as e:
                dest_s3.abort_multipart_upload(
                    Bucket=dest_bucket, Key=dest_key, UploadId=upload_id
                )
                raise RuntimeError(f"upload_part {part_num} failed: {e}") from e

            bytes_done += len(chunk)
            part_num += 1

            now = time.time()
            if now - last_heartbeat >= HEARTBEAT_SEC:
                log(f"{prefix}  Still copying... {bytes_done:,} bytes so far")
                last_heartbeat = now
            if now - last_progress_log >= PROGRESS_INTERVAL_SEC:
                pct = (bytes_done / total_size * 100) if total_size else 100
                elapsed = now - start_time
                rate = f", {bytes_done / elapsed / (1024*1024):.1f} MB/s" if elapsed > 0 else ""
                log(f"{prefix}  {bytes_done:,} / {total_size:,} ({pct:.1f}%){rate}")
                last_progress_log = now
                last_heartbeat = now

        if not parts:
            dest_s3.abort_multipart_upload(
                Bucket=dest_bucket, Key=dest_key, UploadId=upload_id
            )
            raise RuntimeError("Source object is empty")

        dest_s3.complete_multipart_upload(
            Bucket=dest_bucket,
            Key=dest_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except Exception:
        try:
            dest_s3.abort_multipart_upload(
                Bucket=dest_bucket, Key=dest_key, UploadId=upload_id
            )
        except Exception:
            pass
        raise

    # Verify size
    try:
        dest_head = dest_s3.head_object(Bucket=dest_bucket, Key=dest_key)
        dest_size = dest_head["ContentLength"]
    except ClientError as e:
        raise RuntimeError(f"Destination head_object (verify) failed: {e}") from e

    if dest_size != total_size:
        raise RuntimeError(
            f"Size mismatch after copy: source {total_size}, dest {dest_size}"
        )
    log(f"{prefix}  Done. Verified size {dest_size:,} bytes.")


def main():
    parser = argparse.ArgumentParser(
        description="Copy S3 objects between buckets using different AWS profiles (streaming; safe for very large files).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Source S3 URI, e.g. s3://bucket/key or s3://bucket/prefix/",
    )
    parser.add_argument(
        "--source-profile",
        required=True,
        dest="source_profile",
        help="AWS profile for source bucket",
    )
    parser.add_argument(
        "--dest",
        required=True,
        help="Destination S3 URI, e.g. s3://bucket/key or s3://bucket/prefix/",
    )
    parser.add_argument(
        "--dest-profile",
        required=True,
        dest="dest_profile",
        help="AWS profile for destination bucket",
    )
    parser.add_argument(
        "--part-size",
        type=int,
        default=DEFAULT_PART_BYTES,
        help=f"Part size in bytes for multipart upload (default {DEFAULT_PART_BYTES // (1024*1024)} MB)",
    )
    parser.add_argument(
        "--region",
        default="us-east-2",
        help="AWS region for both clients (default us-east-2)",
    )
    parser.add_argument(
        "--log-file",
        dest="log_file",
        metavar="PATH",
        help="Also append all progress to this file (for unattended runs)",
    )
    args = parser.parse_args()

    source_bucket, source_prefix = parse_s3_url(args.source)
    dest_bucket, dest_prefix = parse_s3_url(args.dest)

    if not source_bucket or not dest_bucket:
        print("Invalid source or destination S3 URI.", file=sys.stderr)
        sys.exit(1)

    log_file = None
    if args.log_file:
        try:
            log_file = open(args.log_file, "a", encoding="utf-8")
        except OSError as e:
            print(f"Cannot open log file: {e}", file=sys.stderr)
            sys.exit(1)

    try:
        config = Config(
            read_timeout=300,
            connect_timeout=60,
            retries={"max_attempts": 5, "mode": "standard"},
        )
        source_session = boto3.Session(profile_name=args.source_profile)
        dest_session = boto3.Session(profile_name=args.dest_profile)
        source_s3 = source_session.client("s3", region_name=args.region, config=config)
        dest_s3 = dest_session.client("s3", region_name=args.region, config=config)

        # Single object vs prefix
        if source_prefix and not source_prefix.endswith("/"):
            # Single object
            keys = [source_prefix]
            dest_prefix_for_key = dest_prefix  # dest_key = dest_prefix (might include filename)
        else:
            # List all under prefix
            keys = list_all_objects(source_s3, source_bucket, source_prefix or "")
            if not keys:
                print("No objects found under source prefix.", file=sys.stderr)
                sys.exit(1)
            # Preserve structure: dest_key = dest_prefix + relative_path
            dest_prefix = (dest_prefix or "").rstrip("/")
            if dest_prefix:
                dest_prefix = dest_prefix + "/"

        total = len(keys)
        for i, src_key in enumerate(keys, 1):
            if total > 1:
                # Preserve path under source prefix
                rel = src_key[len(source_prefix or "") :].lstrip("/") if (source_prefix or "").rstrip("/") else src_key
                dest_key = (dest_prefix + rel) if dest_prefix else rel
            else:
                dest_key = dest_prefix if dest_prefix else src_key

            copy_one_object(
                source_s3,
                source_bucket,
                src_key,
                dest_s3,
                dest_bucket,
                dest_key,
                args.part_size,
                log_file,
                file_index=i,
                total_files=total,
            )
    except Exception as e:
        if log_file:
            log_file.write(f"[{datetime.utcnow():%Y-%m-%d %H:%M:%S}] ERROR: {e}\n")
            log_file.flush()
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if log_file:
            log_file.close()

    print("All objects copied and verified.", flush=True)


if __name__ == "__main__":
    main()
