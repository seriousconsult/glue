# Data Engineering

Scripts to upload large amounts of data to S3. Main use: create a transfer EC2 from AWS CloudShell, connect with Session Manager, then run the upload script on the instance.

---

## Quick start: Upload files to S3

**1. Create the transfer server** (in AWS CloudShell)

```bash
python create_transfer_ec2.py
```

This starts an 8-core Amazon Linux EC2 in us-east-2 with 1.5 TB storage at `/data`. When it finishes, it prints a connect command.

**2. Connect to the instance** (same CloudShell)

```bash
aws ssm start-session --target <instance-id> --region us-east-2
```

No SSH key. Uses Session Manager.

**Optional: copy your AWS profiles to the EC2** (one command, no paste)

In CloudShell, after the instance is running:  
`python3 copy_aws_profiles.py`  
(Or pass the instance ID: `python3 copy_aws_profiles.py i-xxxxx`.)  
Profiles are pushed via S3 + SSM; the instance gets the same ~/.aws as CloudShell.

**3. Upload files**

Put your files in `/data` on the instance, then run:

```bash
python3 setup_server_to_s3.py
```

That downloads the latest upload script from S3 and runs it (default: uploads `/home/ec2-user/data` to bucket `kindred-0`). To use `/data` instead:

```bash
python3 setup_server_to_s3.py --path /data
```

---

## What each script does

| Script | What it does |
|--------|----------------|
| **create_transfer_ec2.py** | Creates the EC2. Run once from CloudShell. |
| **setup_server_to_s3.py** or **.sh** | Downloads `server_to_s3.py` from S3, installs deps, runs it. Use on the EC2 when you don’t have the script there. |
| **server_to_s3.py** | Does the upload (local folder → S3). Run it on the machine that has the files. |
| **copy_aws_profiles.py** | Push AWS profiles from CloudShell to the EC2: run `python3 copy_aws_profiles.py` (no paste). |
| **s3_cross_copy.py** | Copy S3 objects between buckets with different AWS profiles (streaming; for very large files). Preserves folder structure; progress + optional log file for unattended runs. |
| **join.py** | S3 data join / streaming job. |
| **un-parquet.py**, **uncompress.py** | Parquet and compression helpers. |

---

## server_to_s3.py — options

Install on the server: `pip install -r requirements.txt`

**Examples**

```bash
python server_to_s3.py                                    # default path, default bucket
python server_to_s3.py --path /data                      # upload from /data
python server_to_s3.py --bucket my-bucket --s3-prefix x/   # bucket and prefix
python server_to_s3.py --max-workers 100 --verbose        # more threads, verbose
```

**Options**

| Option | Default | Meaning |
|--------|---------|---------|
| `--path` | `/home/ec2-user/data` | Local folder to upload |
| `--bucket` | `kindred-0` | S3 bucket name |
| `--s3-prefix` | (none) | Prefix (folder) in S3 |
| `--profile` | (none) | AWS profile |
| `--max-workers` | 50 | Concurrent uploads |
| `--verbose` / `-v` | off | Verbose logging |

---

## setup_server_to_s3 — fetch and run

Use when the EC2 (or any machine) doesn’t have `server_to_s3.py` yet. It pulls the script and `requirements.txt` from `s3://kindred-0/`, installs, runs, then cleans up. Any extra arguments are passed to `server_to_s3.py`.

**Python**

```bash
python3 setup_server_to_s3.py --path /data
```

**Bash**

```bash
chmod +x setup_server_to_s3.sh
./setup_server_to_s3.sh --path /data
```

---

## s3_cross_copy.py — S3 to S3 with two profiles

Copies objects from one bucket (one AWS profile) to another (another profile). Source and destination are hardcoded in the script. Streams through the host so it works for very large objects (e.g. 1TB). Preserves folder structure. No silent failures; progress is always written to `s3_cross_copy.log`.

**Run:**
```bash
python3 s3_cross_copy.py
```

**Unattended on transfer EC2 (run and disconnect):**
```bash
nohup python3 s3_cross_copy.py >> s3_cross_copy.log 2>&1 &
tail -f s3_cross_copy.log
```

---

## Prerequisites

- **AWS:** Credentials (e.g. `aws configure`) or IAM role on EC2.
- **Transfer EC2:** Default VPC is fine; instance needs outbound access for Session Manager.
