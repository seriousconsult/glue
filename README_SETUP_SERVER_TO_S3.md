# Setup Scripts for server_to_s3

These scripts download the latest versions of `server_to_s3.py` and `requirements.txt` from the S3 bucket before running.

## Files

- `setup_server_to_s3.py` - Python setup script (uses boto3)
- `setup_server_to_s3.sh` - Bash setup script (uses AWS CLI)

## Usage

### Python (setup_server_to_s3.py)

**Prerequisites:**
- Python 3.7+
- boto3 installed (`pip install boto3`)
- AWS credentials configured

**Run:**
```bash
python3 setup_server_to_s3.py [arguments for server_to_s3.py]
```

**Example:**
```bash
# Use defaults
python3 setup_server_to_s3.py

# With custom path
python3 setup_server_to_s3.py --path /home/user/data

# With S3 prefix
python3 setup_server_to_s3.py --s3-prefix backups/2024-01-01
```

### Bash (setup_server_to_s3.sh)

**Prerequisites:**
- AWS CLI installed and configured
- Python 3.7+
- pip installed

**Run:**
```bash
chmod +x setup_server_to_s3.sh
./setup_server_to_s3.sh [arguments for server_to_s3.py]
```

**Example:**
```bash
# Use defaults
./setup_server_to_s3.sh

# With custom path
./setup_server_to_s3.sh --path /home/user/data

# With S3 prefix
./setup_server_to_s3.sh --s3-prefix backups/2024-01-01
```

**Note:** The bash script already uses `python3` internally.

## How It Works

1. Creates a temporary directory
2. Downloads `requirements.txt` from `s3://kindred-0/requirements.txt`
3. Downloads `server_to_s3.py` from `s3://kindred-0/server_to_s3.py`
4. Installs the requirements
5. Executes `server_to_s3.py` with your provided arguments
6. Cleans up the temporary directory

## Benefits

- Always runs the latest version from S3
- No need to manually update files on the server
- Fresh installation of dependencies each time
- Clean execution environment (temporary files)

## Notes

- The setup script passes all command-line arguments to `server_to_s3.py`
- Temporary files are automatically cleaned up after execution
- If download fails, the script exits with an error code
- Make sure AWS credentials are configured (via `aws configure`, environment variables, or IAM role)
