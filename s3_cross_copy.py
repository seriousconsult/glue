#!/usr/bin/env python3
"""
Copy S3 objects from one bucket (with one AWS profile) to another (with another profile).
Source and destination are hardcoded below. Streams through this process—suitable for
very large objects (e.g. 1TB). Preserves folder structure. No silent failures; progress
printed frequently.

Copies up to 16 files in parallel (capped to avoid OOM). For long runs use nohup so session disconnect doesn't kill the job: nohup python3 s3_cross_copy.py >> s3_cross_copy.log 2>&1 &
"""

import argparse
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

# --- Hardcoded source and destination (edit these) ---
# Multiple source paths; all are listed and copied under DEST_S3_URI preserving structure.
# List entire DB3 and DB4 (includes all subfolders: DB4/maid, DB4/cookie, etc.)
SOURCE_S3_URIS = [
    "s3://kindred-datawarehouse-samples/tgcf/MX/DB3",
    "s3://kindred-datawarehouse-samples/tgcf/MX/DB4",
]
# Prefix to strip from each source path to get dest subpath (e.g. tgcf/MX/ -> DB3, DB4/maid under MX2)
SOURCE_BASE = "tgcf/MX/"
SOURCE_PROFILE = "kindred"
DEST_S3_URI = "s3://kindred-0/MX2/"
DEST_PROFILE = "default"
REGION = "us-east-2"
# ----------------------------------------------------

# Script self-update: pull latest from this location before running
SCRIPT_S3_BUCKET = "kindred-0"
SCRIPT_S3_KEY = "data_engineering/s3_cross_copy.py"
DEFAULT_LOG_FILE = "s3_cross_copy.log"

# Multipart: S3 allows max 10,000 parts, min part 5MB (except last). For 1TB use 100MB parts.
DEFAULT_PART_BYTES = 100 * 1024 * 1024  # 100 MB
MAX_WORKERS = 16  # Parallel file copies; ~100–200 MB per worker—lower if OOM on small instances
PROGRESS_INTERVAL_SEC = 2   # Print progress at least this often
HEARTBEAT_SEC = 30          # If no part in this long, print "still copying..." so user knows it's not hung


def update_script_from_s3():
    """Download latest s3_cross_copy.py from S3, overwrite self, re-exec. Returns True if we re-exec'd (does not return)."""
    try:
        session = boto3.Session(profile_name=DEST_PROFILE)
        s3 = session.client("s3", region_name=REGION)
        resp = s3.get_object(Bucket=SCRIPT_S3_BUCKET, Key=SCRIPT_S3_KEY)
        new_content = resp["Body"].read()
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            return False
        print(f"Could not fetch latest script from s3://{SCRIPT_S3_BUCKET}/{SCRIPT_S3_KEY}: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Could not fetch latest script: {e}", file=sys.stderr)
        return False

    try:
        with open(__file__, "rb") as f:
            current = f.read()
        if new_content == current:
            return False
    except OSError:
        return False

    try:
        with open(__file__, "wb") as f:
            f.write(new_content)
    except OSError as e:
        print(f"Could not overwrite script (run from writable path?): {e}", file=sys.stderr)
        return False

    print("Updated script from s3://{}/{}; restarting.".format(SCRIPT_S3_BUCKET, SCRIPT_S3_KEY), flush=True)
    os.execv(sys.executable, [sys.executable] + sys.argv + ["--no-update"])
    return True  # unreachable


def parse_s3_url(url):
    """Return (bucket, key). Key may be prefix (trailing /)."""
    url = url.strip().replace("s3://", "", 1)
    if "/" not in url:
        return url, ""
    idx = url.index("/")
    return url[:idx], url[idx + 1 :].lstrip("/")


def list_all_objects(s3, bucket, prefix):
    """List all object keys under prefix (paginated; fetches every page)."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
        PaginationConfig={"PageSize": 1000},
    ):
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
    log_lock=None,
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
        if log_lock:
            with log_lock:
                log_file.write(line + "\n")
                log_file.flush()
        else:
            log_file.write(line + "\n")
            log_file.flush()

    start_time = time.time()
    prefix = f"  [{file_index}/{total_files}] " if file_index is not None else "  "
    log(f"{prefix}Copying s3://{source_bucket}/{source_key} -> s3://{dest_bucket}/{dest_key} ({total_size:,} bytes)")

    # Empty objects: use put_object instead of multipart (multipart requires at least one part)
    if total_size == 0:
        try:
            dest_s3.put_object(Bucket=dest_bucket, Key=dest_key, Body=b"")
        except ClientError as e:
            raise RuntimeError(f"put_object (empty) failed: {e}") from e
        log(f"{prefix}  Done. Verified size 0 bytes (empty object).")
        return

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
        description="Copy S3 objects (source/dest hardcoded). Copies many files in parallel.",
    )
    parser.add_argument(
        "--part-size",
        type=int,
        default=DEFAULT_PART_BYTES,
        help=f"Part size in bytes (default {DEFAULT_PART_BYTES // (1024*1024)} MB)",
    )
    parser.add_argument("--no-update", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if not args.no_update:
        update_script_from_s3()

    dest_bucket, dest_prefix_parsed = parse_s3_url(DEST_S3_URI)
    if not dest_bucket:
        print("Invalid destination S3 URI (check hardcoded vars).", file=sys.stderr)
        sys.exit(1)
    dest_prefix = (dest_prefix_parsed or "").rstrip("/")
    if dest_prefix:
        dest_prefix = dest_prefix + "/"

    try:
        log_file = open(DEFAULT_LOG_FILE, "a", encoding="utf-8")
    except OSError as e:
        print(f"Cannot open log file {DEFAULT_LOG_FILE}: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        config = Config(
            read_timeout=600,
            connect_timeout=60,
            retries={"max_attempts": 5, "mode": "standard"},
            max_pool_connections=64,
        )
        source_session = boto3.Session(profile_name=SOURCE_PROFILE)
        dest_session = boto3.Session(profile_name=DEST_PROFILE)
        source_s3 = source_session.client("s3", region_name=REGION, config=config)
        dest_s3 = dest_session.client("s3", region_name=REGION, config=config)

        source_bucket = None
        tasks = []
        source_summaries = []

        for source_uri in SOURCE_S3_URIS:
            bucket, prefix = parse_s3_url(source_uri)
            if not bucket:
                print(f"Invalid source URI: {source_uri}", file=sys.stderr)
                sys.exit(1)
            if source_bucket is None:
                source_bucket = bucket
            elif source_bucket != bucket:
                print(f"All sources must use the same bucket (got {bucket} and {source_bucket}).", file=sys.stderr)
                sys.exit(1)

            # Use trailing slash so we list only objects under this path (all subfolders)
            list_prefix = (prefix or "").rstrip("/")
            if list_prefix:
                list_prefix = list_prefix + "/"
            keys = list_all_objects(source_s3, bucket, list_prefix)
            pre = list_prefix.rstrip("/")  # base path for relative keys
            if pre:
                folders = sorted(set(k[len(pre):].lstrip("/").split("/")[0] for k in keys if len(k) > len(pre)))
            else:
                folders = sorted(set(k.split("/")[0] for k in keys))
            source_summaries.append((source_uri, len(keys), folders))

            dest_subpath = pre[len(SOURCE_BASE):].lstrip("/") if (SOURCE_BASE and pre.startswith(SOURCE_BASE)) else pre
            if dest_subpath:
                dest_subpath = dest_subpath.rstrip("/") + "/"

            for src_key in keys:
                rel = src_key[len(pre):].lstrip("/") if pre else src_key
                dkey = (dest_prefix + dest_subpath + rel) if dest_subpath else (dest_prefix + rel) if dest_prefix else rel
                tasks.append((src_key, dkey))

        total = len(tasks)
        if total == 0:
            print("No objects found under any source prefix.", file=sys.stderr)
            sys.exit(1)

        # Before any copy: list all folders found in the source bucket
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        log_file.write(f"[{ts}] === Folders found in source bucket (before copy) ===\n")
        log_file.flush()
        print(f"[{ts}] === Folders found in source bucket (before copy) ===", flush=True)
        for source_uri, count, folders in source_summaries:
            folder_list = ", ".join(folders[:20]) + (" ..." if len(folders) > 20 else "")
            line = f"  {source_uri}: {count} objects, folders: {folder_list}"
            log_file.write(f"[{ts}] {line}\n")
            log_file.flush()
            print(f"  {source_uri}: {count} objects, folders: {folder_list}", flush=True)
        total_line = f"  Total: {total} objects. Starting copy."
        log_file.write(f"[{ts}] {total_line}\n")
        log_file.flush()
        print(f"[{ts}] {total_line}", flush=True)

        reply = input("Proceed with copy? (y/n): ").strip().lower()
        if reply not in ("y", "yes"):
            print("Copy cancelled.", flush=True)
            log_file.write(f"[{datetime.utcnow():%Y-%m-%d %H:%M:%S}] Copy cancelled by user.\n")
            log_file.flush()
            sys.exit(0)

        tasks = [(src_key, dkey, i) for i, (src_key, dkey) in enumerate(tasks, 1)]

        workers = min(MAX_WORKERS, total)
        log_lock = threading.Lock() if workers > 1 else None

        def copy_task(item):
            src_key, dkey, i = item
            return copy_one_object(
                source_s3,
                source_bucket,
                src_key,
                dest_s3,
                dest_bucket,
                dkey,
                args.part_size,
                log_file,
                log_lock=log_lock,
                file_index=i,
                total_files=total,
            )

        if workers == 1:
            for item in tasks:
                copy_task(item)
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(copy_task, item): item for item in tasks}
                try:
                    for future in as_completed(futures):
                        future.result()
                except KeyboardInterrupt:
                    done = sum(1 for f in futures if f.done())
                    if done == total:
                        for f in futures:
                            f.result()
                        print("All objects copied and verified.", flush=True)
                        sys.exit(0)
                    else:
                        log_file.write(
                            f"[{datetime.utcnow():%Y-%m-%d %H:%M:%S}] Interrupted ({done}/{total} completed). "
                            "Use nohup for long runs: nohup python3 s3_cross_copy.py >> s3_cross_copy.log 2>&1 &\n"
                        )
                        log_file.flush()
                        print(f"Interrupted ({done}/{total} completed). Use nohup for long runs.", file=sys.stderr)
                        sys.exit(1)
    except KeyboardInterrupt:
        log_file.write(f"[{datetime.utcnow():%Y-%m-%d %H:%M:%S}] Interrupted. Use nohup for long runs.\n")
        log_file.flush()
        print("Interrupted. Use nohup for long runs: nohup python3 s3_cross_copy.py >> s3_cross_copy.log 2>&1 &", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        log_file.write(f"[{datetime.utcnow():%Y-%m-%d %H:%M:%S}] ERROR: {e}\n")
        log_file.flush()
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        log_file.close()

    print("All objects copied and verified.", flush=True)


if __name__ == "__main__":
    main()
