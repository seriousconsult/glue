#!/usr/bin/env python3
"""
Script to copy all files and folders from the local server to an S3 bucket.
Run this script on the server where the files are located.
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
import tempfile
import shutil
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
from tqdm import tqdm


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ServerToS3Copier:
    """Handles copying files from local filesystem to S3 bucket."""
    
    def __init__(
        self,
        local_path: str,
        s3_bucket: str,
        s3_prefix: str = "",
        aws_profile: Optional[str] = None,
        max_workers: int = 50
    ):
        """
        Initialize the copier.
        
        Args:
            local_path: Source path on the local filesystem
            s3_bucket: Destination S3 bucket name
            s3_prefix: Optional S3 key prefix (folder path in S3)
            aws_profile: AWS profile name (optional)
            max_workers: Maximum number of concurrent upload threads (default: 50)
        """
        self.local_path = os.path.abspath(local_path)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.rstrip('/')
        self.aws_profile = aws_profile
        self.max_workers = max_workers
        
        self.s3_client = None
        # Configure transfer settings for optimal performance with more connections
        # Note: max_concurrency is for multipart uploads within a single file, not for multiple files
        self.transfer_config = TransferConfig(
            multipart_threshold=16 * 1024 * 1024,  # 16MB - use multipart for files larger than this (lower threshold = more parallelism)
            max_concurrency=20,  # Increased: More threads for multipart uploads of a single file
            multipart_chunksize=16 * 1024 * 1024,  # 16MB chunks for multipart uploads
            use_threads=True,  # Use threads for parallel parts
            max_bandwidth=None  # No bandwidth limit
        )
        
    def connect_s3(self):
        """Initialize S3 client with Transfer Acceleration enabled."""
        try:
            session = boto3.Session(profile_name=self.aws_profile) if self.aws_profile else boto3.Session()
            # Enable S3 Transfer Acceleration for faster uploads (uses s3-accelerate.amazonaws.com endpoint)
            # Increase connection pool size for better performance with concurrent uploads
            config = Config(
                s3={'use_accelerate_endpoint': True},
                max_pool_connections=200  # Large connection pool for maximum concurrent uploads
            )
            self.s3_client = session.client('s3', region_name='us-east-2', config=config)
            
            # Test connection by checking bucket access
            self.s3_client.head_bucket(Bucket=self.s3_bucket)
            logger.info(f"Successfully connected to S3 bucket: {self.s3_bucket} (using Transfer Acceleration)")
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"S3 bucket '{self.s3_bucket}' does not exist")
            elif error_code == '403':
                logger.error(f"Access denied to S3 bucket '{self.s3_bucket}'")
            else:
                logger.error(f"Failed to connect to S3: {e}")
            raise
        except BotoCoreError as e:
            logger.error(f"AWS credentials error: {e}")
            raise
    
    def get_local_files(self, local_path: str):
        """
        Recursively get all files from the local filesystem.
        Returns .gz files for processing, and regular files only if no .gz version exists.
        
        Yields:
            Full file paths
        """
        processed_files = set()  # Track files we've already processed
        
        try:
            for root, dirs, files in os.walk(local_path):
                # Skip hidden directories
                dirs[:] = [d for d in dirs if not d.startswith('.')]
                
                for file in files:
                    # Skip hidden files
                    if file.startswith('.'):
                        continue
                    
                    file_path = os.path.join(root, file)
                    
                    if file_path.endswith('.gz'):
                        # Yield .gz files - they will be uploaded uncompressed
                        yield file_path
                        processed_files.add(file_path)
                    else:
                        # Check if there's a corresponding .gz file
                        gz_path = file_path + '.gz'
                        if os.path.exists(gz_path):
                            # .gz version exists, skip the regular file
                            # (it will be uploaded uncompressed from the .gz file)
                            continue
                        # No .gz version, yield the regular file
                        yield file_path
                        processed_files.add(file_path)
                    
        except PermissionError as e:
            logger.warning(f"Permission denied accessing {local_path}: {e}")
        except Exception as e:
            logger.error(f"Error accessing {local_path}: {e}")
    
    def upload_file_to_s3(self, local_file_path: str):
        """
        Upload a single file from local filesystem to S3.
        Handles .gz files by uncompressing, uploading, and cleaning up.
        
        Args:
            local_file_path: Full path to the local file
            
        Returns:
            'uploaded' if file was uploaded, 'skipped' if skipped
        """
        try:
            # Check if this is a compressed file
            if local_file_path.endswith('.gz'):
                return self._upload_uncompressed_gz(local_file_path)
            
            # Calculate relative path from local_path
            rel_path = os.path.relpath(local_file_path, self.local_path)
            
            # Determine S3 key
            s3_key = f"{self.s3_prefix}/{rel_path}" if self.s3_prefix else rel_path
            s3_key = s3_key.replace('\\', '/')  # Normalize path separators for S3
            
            # Get file size
            file_size = os.path.getsize(local_file_path)
            
            # Check if file already exists and is the same size
            try:
                head_response = self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
                if head_response.get('ContentLength') == file_size:
                    return 'skipped'
            except ClientError:
                # File doesn't exist, proceed with upload
                pass
            
            # Upload file to S3 with optimized transfer config
            self.s3_client.upload_file(
                local_file_path,
                self.s3_bucket,
                s3_key,
                ExtraArgs={'Metadata': {'source': local_file_path}},
                Config=self.transfer_config
            )
            
            return 'uploaded'
            
        except Exception as e:
            logger.error(f"Failed to upload {local_file_path}: {e}")
            raise
    
    def _upload_uncompressed_gz(self, gz_file_path: str):
        """
        Uncompress a .gz file, upload the uncompressed version, and clean up.
        
        Args:
            gz_file_path: Full path to the .gz file
            
        Returns:
            'uploaded' if file was uploaded, 'skipped' if skipped
        """
        temp_uncompressed = None
        try:
            # Calculate relative path from local_path (without .gz extension)
            rel_path = os.path.relpath(gz_file_path, self.local_path)
            # Remove .gz extension for S3 key
            rel_path_uncompressed = rel_path[:-3] if rel_path.endswith('.gz') else rel_path
            
            # Determine S3 key (without .gz extension)
            s3_key = f"{self.s3_prefix}/{rel_path_uncompressed}" if self.s3_prefix else rel_path_uncompressed
            s3_key = s3_key.replace('\\', '/')  # Normalize path separators for S3
            
            # Check if uncompressed file already exists in S3
            try:
                head_response = self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
                # File exists, skip upload
                return 'skipped'
            except ClientError:
                # File doesn't exist, proceed with upload
                pass
            
            # Create temporary file for uncompressed data
            temp_uncompressed = None
            
            try:
                # Use system gunzip for faster decompression (much faster than Python gzip module)
                # gunzip writes to stdout, so we capture it to a temp file
                temp_uncompressed = tempfile.mktemp(prefix='s3_upload_', suffix='.tmp')
                
                # Use gunzip command for faster decompression
                with open(temp_uncompressed, 'wb') as outfile:
                    result = subprocess.run(
                        ['gunzip', '-c', gz_file_path],
                        stdout=outfile,
                        stderr=subprocess.PIPE,
                        check=True
                    )
                
                # Upload uncompressed file to S3
                self.s3_client.upload_file(
                    temp_uncompressed,
                    self.s3_bucket,
                    s3_key,
                    ExtraArgs={'Metadata': {'source': gz_file_path, 'compressed_source': 'yes'}},
                    Config=self.transfer_config
                )
                
                return 'uploaded'
                
            except subprocess.CalledProcessError as e:
                # Fallback to Python gzip if gunzip command fails
                logger.warning(f"gunzip command failed, using Python gzip: {e.stderr.decode() if e.stderr else str(e)}")
                return self._upload_uncompressed_gz_python(gz_file_path, s3_key)
            except FileNotFoundError:
                # gunzip not available, use Python gzip
                logger.warning("gunzip command not found, using Python gzip")
                return self._upload_uncompressed_gz_python(gz_file_path, s3_key)
            finally:
                # Clean up temporary uncompressed file
                if temp_uncompressed and os.path.exists(temp_uncompressed):
                    try:
                        os.remove(temp_uncompressed)
                    except Exception as e:
                        logger.warning(f"Failed to clean up temp file {temp_uncompressed}: {e}")
                        
        except Exception as e:
            # Clean up on error
            if temp_uncompressed and os.path.exists(temp_uncompressed):
                try:
                    os.remove(temp_uncompressed)
                except Exception:
                    pass
            raise
    
    def _upload_uncompressed_gz_python(self, gz_file_path: str, s3_key: str):
        """
        Fallback method: Uncompress a .gz file using Python gzip module.
        
        Args:
            gz_file_path: Full path to the .gz file
            s3_key: S3 key for the uncompressed file
            
        Returns:
            'uploaded' if file was uploaded, 'skipped' if skipped
        """
        temp_uncompressed = None
        temp_fd = None
        try:
            # Create temporary file for uncompressed data
            temp_fd, temp_uncompressed = tempfile.mkstemp(prefix='s3_upload_', suffix='.tmp')
            
            # Uncompress using Python gzip (fallback)
            import gzip
            with gzip.open(gz_file_path, 'rb') as gz_file:
                with os.fdopen(temp_fd, 'wb') as uncompressed_file:
                    temp_fd = None  # File is now managed by context manager
                    # Read and write in chunks to handle large files
                    shutil.copyfileobj(gz_file, uncompressed_file, length=16*1024*1024)
            
            # Upload uncompressed file to S3
            self.s3_client.upload_file(
                temp_uncompressed,
                self.s3_bucket,
                s3_key,
                ExtraArgs={'Metadata': {'source': gz_file_path, 'compressed_source': 'yes'}},
                Config=self.transfer_config
            )
            
            return 'uploaded'
            
        finally:
            # Clean up temporary uncompressed file
            if temp_fd is not None:
                try:
                    os.close(temp_fd)
                except Exception:
                    pass
            if temp_uncompressed and os.path.exists(temp_uncompressed):
                try:
                    os.remove(temp_uncompressed)
                except Exception as e:
                    logger.warning(f"Failed to clean up temp file {temp_uncompressed}: {e}")
    
    def copy_all(self):
        """Main method to copy all files from local filesystem to S3."""
        try:
            # Connect to S3
            self.connect_s3()
            
            # Verify local path exists
            if not os.path.exists(self.local_path):
                logger.error(f"Local path {self.local_path} does not exist")
                return
            
            if not os.path.isdir(self.local_path):
                logger.error(f"Local path {self.local_path} is not a directory")
                return
            
            logger.info(f"Starting copy operation from {self.local_path} to s3://{self.s3_bucket}/{self.s3_prefix}")
            
            # Collect all files first to show total count
            logger.info("Scanning files...")
            all_files = list(self.get_local_files(self.local_path))
            total_files = len(all_files)
            
            if total_files == 0:
                logger.warning("No files found to upload")
                return
            
            logger.info(f"Found {total_files} files to process")
            logger.info(f"Using {self.max_workers} concurrent upload threads")
            
            # Upload files concurrently with progress bar
            uploaded_count = 0
            skipped_count = 0
            error_count = 0
            
            with tqdm(total=total_files, desc="Uploading files", unit="file", 
                     bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # Submit all upload tasks
                    future_to_file = {
                        executor.submit(self.upload_file_to_s3, file_path): file_path 
                        for file_path in all_files
                    }
                    
                    # Process completed uploads as they finish
                    for future in as_completed(future_to_file):
                        local_file_path = future_to_file[future]
                        rel_path = os.path.relpath(local_file_path, self.local_path)
                        pbar.set_postfix(file=os.path.basename(rel_path)[:30])
                        
                        try:
                            result = future.result()
                            if result == 'skipped':
                                skipped_count += 1
                            elif result == 'uploaded':
                                uploaded_count += 1
                            else:
                                uploaded_count += 1
                        except Exception as e:
                            error_count += 1
                            logger.error(f"Failed to upload {local_file_path}: {e}")
                        
                        pbar.update(1)
            
            # Summary (use print to avoid interfering with tqdm)
            print(f"\n{'='*60}")
            print(f"Copy operation completed!")
            print(f"  Total files: {total_files}")
            print(f"  Uploaded: {uploaded_count}")
            print(f"  Skipped (already exist): {skipped_count}")
            if error_count > 0:
                print(f"  Errors: {error_count}")
            print(f"{'='*60}")
            
        except Exception as e:
            logger.error(f"Copy operation failed: {e}")
            raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Copy all files and folders from local filesystem to S3 bucket',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage (uses defaults: /home/ec2-user/data and bucket kindred-0):
  python server_to_s3.py
  
  # With custom path:
  python server_to_s3.py --path /home/user/data
  
  # With custom bucket:
  python server_to_s3.py --bucket my-bucket
  
  # With S3 prefix (folder):
  python server_to_s3.py --s3-prefix backups/2024-01-01
  
  # Using AWS profile:
  python server_to_s3.py --profile my-aws-profile
        """
    )
    
    parser.add_argument('--path', default='/home/ec2-user/data', help='Source path on the local filesystem (default: /home/ec2-user/data)')
    parser.add_argument('--bucket', default='kindred-0', help='Destination S3 bucket name (default: kindred-0)')
    parser.add_argument('--s3-prefix', default='', help='S3 key prefix (optional folder path in S3)')
    parser.add_argument('--profile', help='AWS profile name (optional)')
    parser.add_argument('--max-workers', type=int, default=50, help='Maximum number of concurrent upload threads (default: 50)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        copier = ServerToS3Copier(
            local_path=args.path,
            s3_bucket=args.bucket,
            s3_prefix=args.s3_prefix,
            aws_profile=args.profile,
            max_workers=args.max_workers
        )
        copier.copy_all()
        
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
