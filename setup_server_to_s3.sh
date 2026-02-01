#!/bin/bash
# Setup script: download server_to_s3.py and requirements.txt from S3
# and execute the script with fresh copies.

set -e

S3_BUCKET="kindred-0"
S3_SCRIPT_KEY="server_to_s3.py"
S3_REQUIREMENTS_KEY="requirements.txt"
AWS_REGION="us-east-2"
TEMP_DIR=$(mktemp -d -t server_to_s3_XXXXXX)

# Cleanup function
cleanup() {
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

echo "Downloading files from s3://${S3_BUCKET}/"
echo "------------------------------------------------------------"

# Download requirements.txt
echo "Downloading ${S3_REQUIREMENTS_KEY}..."
if ! aws s3 cp "s3://${S3_BUCKET}/${S3_REQUIREMENTS_KEY}" "${TEMP_DIR}/${S3_REQUIREMENTS_KEY}" --region "${AWS_REGION}"; then
    echo "Error: Failed to download ${S3_REQUIREMENTS_KEY}"
    exit 1
fi

# Download server_to_s3.py
echo "Downloading ${S3_SCRIPT_KEY}..."
if ! aws s3 cp "s3://${S3_BUCKET}/${S3_SCRIPT_KEY}" "${TEMP_DIR}/${S3_SCRIPT_KEY}" --region "${AWS_REGION}"; then
    echo "Error: Failed to download ${S3_SCRIPT_KEY}"
    exit 1
fi

# Install requirements
echo "Installing requirements..."
if ! pip install -q -r "${TEMP_DIR}/${S3_REQUIREMENTS_KEY}"; then
    echo "Error: Failed to install requirements"
    exit 1
fi

echo "------------------------------------------------------------"
echo "Running ${S3_SCRIPT_KEY}..."
echo "============================================================"

# Execute the script with all passed arguments
python3 "${TEMP_DIR}/${S3_SCRIPT_KEY}" "$@"
