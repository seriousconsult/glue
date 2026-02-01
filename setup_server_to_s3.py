#!/usr/bin/env python3
"""
Setup script: download server_to_s3.py and requirements.txt from S3,
install dependencies, and run server_to_s3.py with fresh copies.
"""

import os
import sys
import subprocess
import tempfile
import shutil

# Try to import boto3, install if not available
try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
except ImportError:
    print("boto3 not found. Installing boto3...")
    python_cmd = 'python3' if shutil.which('python3') else sys.executable
    result = subprocess.run(
        [python_cmd, '-m', 'pip', 'install', '--quiet', 'boto3'],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print("Error: Failed to install boto3. Please install it manually:")
        print(f"  {python_cmd} -m pip install boto3")
        sys.exit(1)
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError


S3_BUCKET = 'kindred-0'
S3_SCRIPT_KEY = 'server_to_s3.py'
S3_REQUIREMENTS_KEY = 'requirements.txt'
AWS_REGION = 'us-east-2'


def download_from_s3(bucket, key, local_path, region='us-east-2'):
    """Download a file from S3."""
    try:
        s3_client = boto3.client('s3', region_name=region)
        s3_client.download_file(bucket, key, local_path)
        print(f"Downloaded {key} from s3://{bucket}/{key}")
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"Error: File s3://{bucket}/{key} not found")
        elif error_code == '403':
            print(f"Error: Access denied to s3://{bucket}/{key}")
        else:
            print(f"Error downloading {key}: {e}")
        return False
    except BotoCoreError as e:
        print(f"AWS credentials error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error downloading {key}: {e}")
        return False


def install_requirements(requirements_path):
    """Install requirements from requirements.txt."""
    try:
        print(f"Installing requirements from {requirements_path}...")
        # Use python3 explicitly
        python_cmd = 'python3' if shutil.which('python3') else sys.executable
        result = subprocess.run(
            [python_cmd, '-m', 'pip', 'install', '-q', '-r', requirements_path],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print("Requirements installed successfully")
            return True
        else:
            print(f"Error installing requirements: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error installing requirements: {e}")
        return False


def main():
    """Download from S3, install deps, and run server_to_s3.py."""
    # Create temporary directory for downloaded files
    temp_dir = tempfile.mkdtemp(prefix='server_to_s3_')
    
    try:
        script_path = os.path.join(temp_dir, 'server_to_s3.py')
        requirements_path = os.path.join(temp_dir, 'requirements.txt')
        
        print(f"Downloading files from s3://{S3_BUCKET}/")
        print("-" * 60)
        
        # Download requirements.txt
        if not download_from_s3(S3_BUCKET, S3_REQUIREMENTS_KEY, requirements_path, AWS_REGION):
            print("Failed to download requirements.txt")
            return 1
        
        # Download server_to_s3.py
        if not download_from_s3(S3_BUCKET, S3_SCRIPT_KEY, script_path, AWS_REGION):
            print("Failed to download server_to_s3.py")
            return 1
        
        # Install requirements
        if not install_requirements(requirements_path):
            print("Failed to install requirements")
            return 1
        
        # Make script executable
        os.chmod(script_path, 0o755)
        
        print("-" * 60)
        print(f"Running {S3_SCRIPT_KEY}...")
        print("=" * 60)
        
        # Execute the script with all passed arguments (use python3 explicitly)
        python_cmd = 'python3' if shutil.which('python3') else sys.executable
        cmd = [python_cmd, script_path] + sys.argv[1:]
        result = subprocess.run(cmd)
        
        return result.returncode
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        return 1
    except Exception as e:
        print(f"Setup error: {e}")
        return 1
    finally:
        # Cleanup temporary directory
        try:
            shutil.rmtree(temp_dir)
        except Exception:
            pass


if __name__ == '__main__':
    sys.exit(main())
