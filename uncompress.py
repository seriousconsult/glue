import boto3
import gzip
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from boto3.s3.transfer import TransferConfig
from botocore.config import Config

# --- SETTINGS ---
BUCKET_NAME = 'kindred-0'
PREFIX = 'MX/DB4/2025-12-04/'
# Set workers to exactly the number of vCPUs (usually 4 for your instance)
MAX_WORKERS = 8  # Over-subscribe slightly to keep the CPU 100% busy during network waits

def get_s3_client():
    """Each process needs its own client instance"""
    s3_config = Config(
        max_pool_connections=25, 
        s3={'use_accelerate_endpoint': True},
        retries={'max_attempts': 5, 'mode': 'standard'}
    )
    return boto3.client('s3', config=s3_config)

def process_file_multiprocessing(file_key):
    """The Heavy Lifter"""
    # Create client inside the process
    s3_client = get_s3_client()
    dest_key = file_key.replace('.gz', '')
    tag_key, tag_val = 'Decompressed', 'True'

    try:
        # 1. Quick Tag Check
        tagging = s3_client.get_object_tagging(Bucket=BUCKET_NAME, Key=file_key)
        if any(t['Key'] == tag_key and t['Value'] == tag_val for t in tagging.get('TagSet', [])):
            return f"Skipped: {file_key}"

        # 2. Stream, Decompress, and Upload
        # We use a larger part size for multiprocessing to maximize throughput
        transfer_config = TransferConfig(multipart_threshold=1024*20, max_concurrency=10)
        
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
        with gzip.GzipFile(fileobj=response['Body']) as decompressed:
            s3_client.upload_fileobj(
                Fileobj=decompressed,
                Bucket=BUCKET_NAME,
                Key=dest_key,
                Config=transfer_config
            )

        # 3. Final Tagging
        s3_client.put_object_tagging(
            Bucket=BUCKET_NAME, Key=file_key,
            Tagging={'TagSet': [{'Key': tag_key, 'Value': tag_val}]}
        )
        return f"Finished: {file_key}"

    except Exception as e:
        return f"Error: {file_key} | {e}"

def main():
    s3_client = get_s3_client()
    paginator = s3_client.get_paginator('list_objects_v2')
    
    # 1. Faster Discovery
    print("Scanning for files...")
    files = [
        obj['Key'] for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX)
        for obj in page.get('Contents', []) if obj['Key'].endswith('.gz')
    ]

    # 2. Multiprocessing Execution
    print(f"🚀 Launching {MAX_WORKERS} parallel processes for {len(files)} files...")
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(process_file_multiprocessing, files))

    print("\nBatch Complete.")

if __name__ == "__main__":
    main()