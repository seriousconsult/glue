# Server to S3 File Copier

A Python script to copy all files and folders from the local filesystem to an AWS S3 bucket. **Run this script directly on the server where the files are located.**

## Features

- Recursively copies all files and directories from local filesystem to S3
- Preserves directory structure in S3
- Automatically uncompresses .gz files before uploading (keeps original .gz files)
- Concurrent uploads for faster performance (configurable thread count)
- Skips files that already exist with the same size (basic deduplication)
- Comprehensive error handling and logging
- Progress bar with real-time upload statistics
- Skips hidden files and directories

## Requirements

- Python 3.7+
- Run the script on the server where files are located
- AWS credentials configured (via AWS CLI, environment variables, or IAM role)

## Installation

1. Copy the script to your server
2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure AWS credentials (one of the following):
   - Run `aws configure` to set up credentials
   - Set environment variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
   - Use an IAM role if running on EC2

## Usage

### Basic Usage (uses defaults: /home/ec2-user/data and bucket kindred-0):

```bash
python server_to_s3.py
```

### With Custom Path:

```bash
python server_to_s3.py --path /home/user/data
```

### With Custom Bucket:

```bash
python server_to_s3.py --bucket my-s3-bucket
```

### With S3 Prefix (organize files in a folder):

```bash
python server_to_s3.py --s3-prefix backups/2024-01-01
```

### Using AWS Profile:

```bash
python server_to_s3.py --profile my-aws-profile
```

### Verbose Logging:

```bash
python server_to_s3.py --verbose
```

### Concurrent Uploads:

```bash
# Use 100 concurrent upload threads (default is 50)
python server_to_s3.py --max-workers 100

# Use fewer threads if needed
python server_to_s3.py --max-workers 20
```

## Command Line Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| Argument | Required | Description |
|----------|----------|-------------|
| `--path` | No | Source path on the local filesystem (default: /home/ec2-user/data) |
| `--bucket` | No | Destination S3 bucket name (default: kindred-0) |
| `--s3-prefix` | No | S3 key prefix (folder path in S3) |
| `--profile` | No | AWS profile name |
| `--max-workers` | No | Maximum number of concurrent upload threads (default: 50) |
| `--verbose` / `-v` | No | Enable verbose logging |

## How It Works

1. Connects to the specified S3 bucket using AWS credentials
2. Recursively walks through all files in the local source directory
3. Uploads files concurrently using multiple threads (default: 50 concurrent uploads)
4. For each file:
   - **.gz files**: Automatically uncompresses, uploads the uncompressed version, then deletes the temporary uncompressed file (original .gz file is kept)
   - **Regular files**: Checks if it already exists in S3 with the same size
   - Uploads the file to S3 if it doesn't exist or has a different size
   - Uses optimized multipart uploads for large files (64MB+)
   - Preserves the directory structure relative to the source path
   - If a .gz version exists, the regular file is skipped (only the uncompressed version is uploaded)

## Notes

- Files are uploaded directly from local filesystem to S3
- Files that already exist in S3 with the same size are skipped
- The script maintains directory structure in S3
- Hidden files and directories (starting with `.`) are skipped
- Error handling continues processing even if individual files fail
- All operations are logged with timestamps

## Security Considerations

- Ensure S3 bucket has appropriate access controls
- Consider using IAM roles instead of access keys when possible (especially on EC2)
- Store AWS credentials securely
- Run the script with appropriate file system permissions
