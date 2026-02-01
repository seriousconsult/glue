import boto3
import io
import sys
import time
import json
from botocore.exceptions import ClientError, BotoCoreError, NoCredentialsError

# S3 paths in format: bucket-name/path/to/file
S3_PATH_1 = 'deposito10/2026/January/VOZ_20260101.csv'
S3_PATH_2 = 'kindred-0/MX/DB4/2025-12-01/id5-mex-m-00000'


def parse_s3_path(s3_path):
    """
    Parse S3 path into bucket and key.
    Format: bucket-name/path/to/file
    """
    parts = s3_path.split('/', 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    else:
        # If no slash, treat entire string as bucket name
        return parts[0], ''


def get_script_s3_location():
    """
    Get the S3 bucket and key for the script.
    Uses the same pattern as EC2 creation.
    """
    try:
        sts = boto3.client('sts')
        account_id = sts.get_caller_identity()['Account']
        script_bucket = f'ec2-scripts-{account_id}'
        script_key = 'ec2-scripts/join.py'  # Fixed key for latest version
        return script_bucket, script_key
    except Exception:
        return None, None


def is_running_on_ec2():
    """
    Check if we're running on an EC2 instance.
    """
    try:
        import socket
        import os
        hostname = socket.gethostname()
        
        # Check multiple indicators
        checks = [
            'ip-' in hostname,
            '.ec2.internal' in hostname,
            hostname.startswith('ec2-'),
            os.path.exists('/sys/hypervisor/uuid'),  # EC2 has this
            os.path.exists('/sys/devices/virtual/dmi/id/product_uuid'),  # EC2 has this
        ]
        
        # Also check if we're in /opt/data_processing (where EC2 installs it)
        script_path = __file__
        if '/opt/data_processing' in script_path:
            return True
            
        return any(checks)
    except:
        return False


def auto_upload_script_to_s3():
    """
    Automatically upload script to S3 when running from CloudShell.
    This ensures EC2 always has access to the latest version.
    """
    on_ec2 = is_running_on_ec2()
    
    if on_ec2:
        print(f"[Script Upload] Running on EC2, skipping upload")
        return False  # Don't upload if already on EC2
    
    print(f"[Script Upload] Running from CloudShell, uploading to S3...")
    
    try:
        s3 = boto3.client('s3')
        script_bucket, script_key = get_script_s3_location()
        
        if not script_bucket or not script_key:
            print(f"[Script Upload] ⚠ Could not determine S3 location")
            return False
        
        print(f"[Script Upload] Uploading to: s3://{script_bucket}/{script_key}")
        
        # Read current script file
        try:
            with open(__file__, 'r') as f:
                script_content = f.read()
            print(f"[Script Upload] Read {len(script_content)} bytes from script")
        except Exception as e:
            print(f"[Script Upload] ✗ Error reading script: {e}")
            return False
        
        try:
            s3.put_object(
                Bucket=script_bucket,
                Key=script_key,
                Body=script_content.encode('utf-8'),
                ContentType='text/x-python'
            )
            print(f"[Script Upload] ✓✓ Script uploaded successfully!")
            print(f"[Script Upload]   EC2 instances will download this version")
            return True
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            print(f"[Script Upload] ✗ Error: {error_code}")
            if error_code == 'NoSuchBucket':
                # Try to create bucket
                print(f"[Script Upload]   Creating bucket...")
                try:
                    region = boto3.Session().region_name or 'us-east-1'
                    if region == 'us-east-1':
                        s3.create_bucket(Bucket=script_bucket)
                    else:
                        s3.create_bucket(
                            Bucket=script_bucket,
                            CreateBucketConfiguration={'LocationConstraint': region}
                        )
                    print(f"[Script Upload]   ✓ Bucket created, uploading...")
                    # Retry upload
                    s3.put_object(
                        Bucket=script_bucket,
                        Key=script_key,
                        Body=script_content.encode('utf-8'),
                        ContentType='text/x-python'
                    )
                    print(f"[Script Upload] ✓✓ Script uploaded successfully!")
                    return True
                except Exception as create_error:
                    print(f"[Script Upload] ✗ Could not create bucket: {create_error}")
                    return False
            return False
    except Exception as e:
        print(f"[Script Upload] ✗ Exception: {e}")
        return False


def update_script_from_s3():
    """
    Download the latest version of the script from S3 and overwrite the local file.
    This ensures the EC2 instance always runs the latest version from CloudShell.
    """
    on_ec2 = is_running_on_ec2()
    
    print(f"[Script Update] Running on EC2: {on_ec2}")
    
    if not on_ec2:
        print(f"[Script Update] Not on EC2, skipping download")
        return False  # Only download if on EC2
    
    print(f"[Script Update] Checking for latest version from CloudShell...")
    
    try:
        s3 = boto3.client('s3')
        script_bucket, script_key = get_script_s3_location()
        
        if not script_bucket or not script_key:
            print(f"[Script Update] ⚠ Could not determine S3 location")
            return False
        
        print(f"[Script Update] S3 location: s3://{script_bucket}/{script_key}")
        
        script_path = __file__  # Path to current script file
        print(f"[Script Update] Current script: {script_path}")
        
        try:
            # Check if file exists in S3 first
            try:
                s3.head_object(Bucket=script_bucket, Key=script_key)
                print(f"[Script Update] ✓ Found script in S3, downloading...")
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    print(f"[Script Update] ⚠ Script not found in S3 yet")
                    print(f"[Script Update]   Run from CloudShell first to upload it")
                    return False
            
            # Download the script from S3
            s3.download_file(script_bucket, script_key, script_path)
            print(f"[Script Update] ✓✓ Downloaded latest script from CloudShell!")
            print(f"[Script Update]   Restarting with new version...")
            
            # Re-execute the script with the new version
            import os
            os.execv(sys.executable, [sys.executable] + sys.argv)
            return True  # This line won't be reached due to execv
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            print(f"[Script Update] ✗ Error downloading: {error_code}")
            if error_code in ['NoSuchKey', 'NoSuchBucket']:
                print(f"[Script Update]   Script not in S3 yet. Run from CloudShell to upload it.")
            return False
        except Exception as e:
            print(f"[Script Update] ✗ Exception: {e}")
            return False
            
    except Exception as e:
        print(f"[Script Update] ✗ Failed to check S3: {e}")
        return False


def stream_and_match():
    # If running on EC2, always download latest from S3 (from CloudShell)
    # If running from CloudShell, auto-upload to S3 first
    if is_running_on_ec2():
        update_script_from_s3()  # This will restart if updated
    else:
        # Auto-upload to S3 so EC2 can get it
        auto_upload_script_to_s3()
    
    try:
        s3 = boto3.client('s3')
    except NoCredentialsError:
        print("Error: AWS credentials not found. Please configure your credentials.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: Failed to initialize S3 client: {e}")
        sys.exit(1)
    
    matches_found = []

    # Parse S3 paths
    phone_bucket, phone_key = parse_s3_path(S3_PATH_1)
    data_bucket, data_key = parse_s3_path(S3_PATH_2)
    
    print(f"Phone numbers file: s3://{phone_bucket}/{phone_key}")
    print(f"Data file: s3://{data_bucket}/{data_key}")

    # 1. Load approximately 10M numbers into a HashSet (approx 500MB RAM in Python)
    print("Step 1: Loading 10M phone numbers into RAM...")
    phone_set = set()
    
    try:
        phone_file = s3.get_object(Bucket=phone_bucket, Key=phone_key)
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == 'NoSuchKey':
            print(f"Error: File '{phone_key}' not found in bucket '{phone_bucket}'")
        elif error_code == 'NoSuchBucket':
            print(f"Error: Bucket '{phone_bucket}' does not exist")
        else:
            print(f"Error: Failed to retrieve phone numbers file from S3: {e}")
        sys.exit(1)
    except BotoCoreError as e:
        print(f"Error: Network/connection issue while accessing S3: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: Unexpected error loading phone numbers: {e}")
        sys.exit(1)
    
    # Using a buffer to read lines efficiently
    try:
        for line in phone_file['Body'].iter_lines():
            try:
                # Using .strip() to handle newlines/spaces
                phone_set.add(line.decode('utf-8').strip())
            except UnicodeDecodeError as e:
                print(f"Warning: Failed to decode line in phone numbers file: {e}. Skipping line.")
                continue
            except Exception as e:
                print(f"Warning: Error processing line in phone numbers file: {e}. Skipping line.")
                continue
    except Exception as e:
        print(f"Error: Failed to read phone numbers file: {e}")
        sys.exit(1)
    
    if not phone_set:
        print("Error: No phone numbers were loaded. Exiting.")
        sys.exit(1)
    
    print(f"Set loaded. Memory check: {len(phone_set)} items.")

    # 2. Stream the 800M row file
    print("Step 2: Streaming 800M rows from S3...")
    print(f"  Opening file: s3://{data_bucket}/{data_key}")
    
    try:
        data_file = s3.get_object(Bucket=data_bucket, Key=data_key)
        print(f"  ✓ File opened successfully")
        print(f"  Content-Length: {data_file.get('ContentLength', 'Unknown')} bytes")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == 'NoSuchKey':
            print(f"Error: File '{data_key}' not found in bucket '{data_bucket}'")
        elif error_code == 'NoSuchBucket':
            print(f"Error: Bucket '{data_bucket}' does not exist")
        else:
            print(f"Error: Failed to retrieve data file from S3: {e}")
        sys.exit(1)
    except BotoCoreError as e:
        print(f"Error: Network/connection issue while accessing S3: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: Unexpected error loading data file: {e}")
        sys.exit(1)
    
    count = 0
    errors_encountered = 0
    sample_lines_shown = 0
    sample_phones_checked = []
    last_progress_time = time.time()
    
    print("\nStarting to stream and process data file...")
    print("  Reading lines from S3 stream...")
    
    # Process line-by-line (streaming)
    try:
        line_iterator = data_file['Body'].iter_lines()
        print("  ✓ Stream iterator created, starting to read lines...\n")
        
        for line_num, line in enumerate(line_iterator, 1):
            try:
                decoded_line = line.decode('utf-8')
                # Assume phone is first column
                if not decoded_line.strip():
                    if count == 0 and line_num <= 10:
                        print(f"  Line {line_num}: (empty line, skipping)")
                    continue  # Skip empty lines
                
                phone_in_row = decoded_line.split(',')[0].strip()
                
                # Show first few sample lines for verification
                if sample_lines_shown < 5:
                    print(f"  [Line {line_num}] Sample: {decoded_line[:150]}")
                    if phone_in_row:
                        print(f"         → Extracted phone: '{phone_in_row}'")
                        in_set = phone_in_row in phone_set
                        print(f"         → In phone set: {in_set}")
                    sample_lines_shown += 1
                    if sample_lines_shown == 5:
                        print("  ... (continuing to process, showing progress updates)\n")
                
                # Track sample phones being checked (first 10 unique)
                if len(sample_phones_checked) < 10 and phone_in_row:
                    if phone_in_row not in sample_phones_checked:
                        sample_phones_checked.append(phone_in_row)
                        in_set = phone_in_row in phone_set
                        if len(sample_phones_checked) <= 5:
                            print(f"  Checking phone '{phone_in_row}' → {'FOUND' if in_set else 'NOT in set'}")
                
                if phone_in_row in phone_set:
                    matches_found.append(decoded_line)
                    if len(matches_found) <= 10:  # Show first 10 matches
                        print(f"  ✓✓ MATCH #{len(matches_found)} found! Phone: '{phone_in_row}'")
                    
                count += 1
                
                # Progress updates
                current_time = time.time()
                if count % 10000 == 0:  # Every 10K rows
                    elapsed = current_time - last_progress_time
                    rate = count / elapsed if elapsed > 0 else 0
                    print(f"  Progress: {count:,} rows processed | {len(matches_found):,} matches | {rate:.0f} rows/sec")
                    last_progress_time = current_time
                elif count % 100000 == 0:  # Every 100K rows (more detailed)
                    elapsed = current_time - last_progress_time
                    rate = count / elapsed if elapsed > 0 else 0
                    print(f"  ═══ {count:,} rows ({count/1000000:.2f}M) | {len(matches_found):,} matches | {rate:.0f} rows/sec ═══")
                elif count % 1000000 == 0:  # Every 1M rows
                    print(f"  ════════════ {count/1000000}M rows processed | {len(matches_found):,} matches ════════════")
                    
            except UnicodeDecodeError as e:
                errors_encountered += 1
                if errors_encountered <= 10:  # Only print first 10 errors
                    print(f"Warning: Failed to decode line {count + 1}: {e}. Skipping line.")
                continue
            except IndexError:
                errors_encountered += 1
                if errors_encountered <= 10:
                    print(f"Warning: Line {count + 1} has no columns. Skipping line.")
                continue
            except Exception as e:
                errors_encountered += 1
                if errors_encountered <= 10:
                    print(f"Warning: Error processing line {count + 1}: {e}. Skipping line.")
                continue
    except Exception as e:
        print(f"Error: Failed to read data file: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"Step 2 COMPLETE!")
    print(f"{'='*60}")
    print(f"  Total rows processed: {count:,}")
    print(f"  Total matches found: {len(matches_found):,}")
    print(f"  Errors encountered: {errors_encountered}")
    
    if errors_encountered > 10:
        print(f"\n⚠ Warning: {errors_encountered} errors encountered during processing.")
    
    # Diagnostic info
    if count == 0:
        print("\n" + "!"*60)
        print("⚠⚠⚠ CRITICAL WARNING: No rows were processed! ⚠⚠⚠")
        print("!"*60)
        print("  Possible issues:")
        print("  - File might be empty")
        print("  - File might not be readable")
        print("  - Stream might have failed silently")
        print("  - File format might be unexpected")
    elif len(matches_found) == 0 and count > 0:
        print("\n" + "-"*60)
        print("⚠ NOTE: No matches found after processing {:,} rows".format(count))
        print("-"*60)
        print("  Possible reasons:")
        print("  - Phone number formats don't match between files")
        print("  - The phone numbers in the data file don't exist in the phone set")
        print("  - There might be whitespace or formatting differences")
        print("  - Column position might be wrong (checking first column)")
        
        if sample_phones_checked:
            print(f"\n  Sample phones from data file (first 5):")
            for i, sample_phone in enumerate(sample_phones_checked[:5], 1):
                print(f"    {i}. '{sample_phone}'")
                # Check if any sample phones exist in set (with variations)
                found_variations = []
                # Try with/without spaces, dashes, etc.
                variations = [
                    sample_phone,
                    sample_phone.replace(' ', ''),
                    sample_phone.replace('-', ''),
                    sample_phone.replace('(', '').replace(')', ''),
                    sample_phone.replace('+', ''),
                ]
                for var in variations:
                    if var in phone_set:
                        found_variations.append(var)
                        break
                if found_variations:
                    print(f"       → FOUND in phone set (as: {found_variations[0]})")
                else:
                    print(f"       → NOT found in phone set")
                    # Show a sample from phone set for comparison
                    if len(phone_set) > 0:
                        sample_from_set = list(phone_set)[:3]
                        print(f"       → Sample from phone set: {sample_from_set}")

    # 3. Save matches back to S3
    if matches_found:
        try:
            output = "\n".join(matches_found)
            # Save to same bucket as data file, with matches_output.csv in the same directory
            output_key = '/'.join(data_key.split('/')[:-1] + ['matches_output.csv']) if '/' in data_key else 'matches_output.csv'
            s3.put_object(Bucket=data_bucket, Key=output_key, Body=output)
            print(f"Done! Found {len(matches_found)} matches.")
            print(f"Output saved to: s3://{data_bucket}/{output_key}")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == 'NoSuchBucket':
                print(f"Error: Bucket '{data_bucket}' does not exist")
            else:
                print(f"Error: Failed to upload matches to S3: {e}")
            sys.exit(1)
        except BotoCoreError as e:
            print(f"Error: Network/connection issue while uploading to S3: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"Error: Unexpected error uploading matches: {e}")
            sys.exit(1)
    else:
        print("No matches found. Nothing to save.")

def find_existing_processing_instance(instance_type=None):
    """
    Find an existing EC2 instance that was created for data processing.
    Looks for instances with the matching Purpose tag.
    
    Args:
        instance_type: Optional instance type to match (if None, matches any type)
    
    Returns:
        dict with instance info if found, None otherwise
    """
    ec2 = boto3.client('ec2')
    
    try:
        # Search for instances with our Purpose tag
        filters = [
            {'Name': 'tag:Purpose', 'Values': ['S3 data join processing']},
            {'Name': 'instance-state-name', 'Values': ['running', 'pending', 'stopping', 'stopped']}
        ]
        
        if instance_type:
            filters.append({'Name': 'instance-type', 'Values': [instance_type]})
        
        response = ec2.describe_instances(Filters=filters)
        
        # Look for running instances first, then pending, then stopped
        instances_by_state = {'running': [], 'pending': [], 'stopped': [], 'stopping': []}
        
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                state = instance['State']['Name']
                if state in instances_by_state:
                    instances_by_state[state].append(instance)
        
        # Prefer running instances
        for state in ['running', 'pending', 'stopped', 'stopping']:
            if instances_by_state[state]:
                instance = instances_by_state[state][0]  # Take the first one found
                instance_id = instance['InstanceId']
                
                # Get IAM role from instance profile
                iam_role_arn = None
                if instance.get('IamInstanceProfile'):
                    profile_arn = instance['IamInstanceProfile'].get('Arn', '')
                    # Extract role name from ARN: arn:aws:iam::account:instance-profile/name
                    # The role name is typically the same as profile name without '-profile'
                    profile_name = profile_arn.split('/')[-1] if '/' in profile_arn else ''
                    if profile_name.endswith('-profile'):
                        role_name = profile_name[:-8]  # Remove '-profile'
                        try:
                            iam = boto3.client('iam')
                            role_response = iam.get_role(RoleName=role_name)
                            iam_role_arn = role_response['Role']['Arn']
                        except Exception:
                            pass
                
                return {
                    'instance_id': instance_id,
                    'instance_type': instance.get('InstanceType', 'unknown'),
                    'state': state,
                    'public_ip': instance.get('PublicIpAddress', 'N/A'),
                    'private_ip': instance.get('PrivateIpAddress', 'N/A'),
                    'iam_role_arn': iam_role_arn,
                    'found_existing': True
                }
        
        return None
        
    except Exception as e:
        print(f"Warning: Error searching for existing instances: {e}")
        return None


def create_ec2_instance_for_processing(
    instance_type='m5.4xlarge',
    key_name=None,
    security_group_ids=None,
    subnet_id=None,
    iam_role_name=None,
    script_content=None
):
    """
    Creates an EC2 instance optimized for running the stream_and_match script.
    The instance will be configured with SSM Session Manager for secure access.
    If an existing instance with matching tags is found, it will be reused instead.
    
    Args:
        instance_type: EC2 instance type (default: m5.4xlarge - 16 vCPU, 64GB RAM)
        iam_role_name: Name for IAM role to create (default: auto-generated)
        script_content: Content of the script to upload (default: reads current file)
    
    Returns:
        dict with instance_id, iam_role_arn, and connection instructions
    """
    ec2 = boto3.client('ec2')
    iam = boto3.client('iam')
    ssm = boto3.client('ssm')
    
    # Check for existing instance first
    print("Checking for existing data processing instance...")
    existing = find_existing_processing_instance(instance_type=instance_type)
    
    if existing:
        print(f"✓ Found existing instance: {existing['instance_id']}")
        print(f"  State: {existing['state']}")
        print(f"  Type: {existing['instance_type']}")
        print(f"  Public IP: {existing['public_ip']}")
        
        if existing['state'] == 'running':
            print("\n✓ Using existing running instance (no new instance created)")
            instance_id = existing['instance_id']
            
            # Check SSM status
            try:
                response = ssm.describe_instance_information(
                    Filters=[{'Key': 'InstanceIds', 'Values': [instance_id]}]
                )
                ssm_ready = len(response['InstanceInformationList']) > 0
            except Exception:
                ssm_ready = False
            
            # Build result similar to new instance creation
            instance_info = {
                'instance_id': instance_id,
                'iam_role_arn': existing.get('iam_role_arn', 'N/A'),
                'instance_type': existing['instance_type'],
                'public_ip': existing['public_ip'],
                'private_ip': existing['private_ip'],
                'ssm_ready': ssm_ready,
                'found_existing': True,
                'ssm_connect_command': f'aws ssm start-session --target {instance_id}',
                'script_location': '/opt/data_processing/join.py',
                'run_command': 'python3 /opt/data_processing/join.py'
            }
            
            # Save to file
            try:
                with open('ec2_instance_info.json', 'w') as f:
                    json.dump(instance_info, f, indent=2)
            except Exception:
                pass
            
            print("\n" + "="*60)
            print("Using Existing EC2 Instance")
            print("="*60)
            print(f"Instance ID: {instance_id}")
            print(f"Instance Type: {existing['instance_type']}")
            print(f"SSM Ready: {'Yes' if ssm_ready else 'Not yet'}")
            print(f"\nTo connect via SSM Session Manager:")
            print(f"  aws ssm start-session --target {instance_id}")
            print(f"\nOnce connected, run the script:")
            print(f"  python3 /opt/data_processing/join.py")
            print("="*60)
            
            return instance_info
            
        elif existing['state'] == 'stopped':
            print(f"\n⚠ Found stopped instance. Starting it...")
            try:
                ec2.start_instances(InstanceIds=[existing['instance_id']])
                print("Instance start requested. Waiting for it to be running...")
                waiter = ec2.get_waiter('instance_running')
                waiter.wait(InstanceIds=[existing['instance_id']], WaiterConfig={'Delay': 5, 'MaxAttempts': 60})
                print("✓ Instance is now running!")
                # Recursively call to get the updated info
                return create_ec2_instance_for_processing(
                    instance_type=instance_type,
                    key_name=key_name,
                    security_group_ids=security_group_ids,
                    subnet_id=subnet_id,
                    iam_role_name=iam_role_name,
                    script_content=script_content
                )
            except Exception as e:
                print(f"Error starting instance: {e}")
                print("Will create a new instance instead...")
        else:
            print(f"\n⚠ Found instance in '{existing['state']}' state.")
            print("Will create a new instance instead...")
    
    print("No suitable existing instance found. Creating new instance...\n")
    
    # Read script content if not provided
    if script_content is None:
        try:
            with open(__file__, 'r') as f:
                script_content = f.read()
        except Exception as e:
            print(f"Warning: Could not read script file: {e}")
            script_content = "# Script will be uploaded separately\n"
    
    # Create IAM role for EC2 with SSM and S3 permissions
    if iam_role_name is None:
        iam_role_name = f'ec2-ssm-role-{int(time.time())}'
    
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "ec2.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    # SSM managed policy ARN
    ssm_managed_policy = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
    
    try:
        # Create IAM role
        print(f"Creating IAM role: {iam_role_name}...")
        role_response = iam.create_role(
            RoleName=iam_role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="EC2 role for SSM Session Manager and S3 access"
        )
        role_arn = role_response['Role']['Arn']
        
        # Attach SSM managed policy
        iam.attach_role_policy(
            RoleName=iam_role_name,
            PolicyArn=ssm_managed_policy
        )
        
        # Create and attach S3 full access policy (or create a more restrictive one)
        s3_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:ListBucket"
                    ],
                    "Resource": "*"
                }
            ]
        }
        
        iam.put_role_policy(
            RoleName=iam_role_name,
            PolicyName='S3Access',
            PolicyDocument=json.dumps(s3_policy)
        )
        
        # Create instance profile
        instance_profile_name = f'{iam_role_name}-profile'
        try:
            iam.create_instance_profile(InstanceProfileName=instance_profile_name)
        except ClientError as e:
            if e.response['Error']['Code'] != 'EntityAlreadyExists':
                raise
        
        # Add role to instance profile (check if already added)
        try:
            # Check if role is already in the profile
            profile_info = iam.get_instance_profile(InstanceProfileName=instance_profile_name)
            roles_in_profile = [r['RoleName'] for r in profile_info['InstanceProfile'].get('Roles', [])]
            
            if iam_role_name not in roles_in_profile:
                print(f"Adding role to instance profile...")
                iam.add_role_to_instance_profile(
                    InstanceProfileName=instance_profile_name,
                    RoleName=iam_role_name
                )
            else:
                print(f"Role already in instance profile.")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'LimitExceeded':
                print("Role already in instance profile (LimitExceeded).")
            elif error_code == 'NoSuchEntity':
                # Profile doesn't exist yet, create it
                print("Instance profile not found, creating...")
                iam.create_instance_profile(InstanceProfileName=instance_profile_name)
                time.sleep(2)
                iam.add_role_to_instance_profile(
                    InstanceProfileName=instance_profile_name,
                    RoleName=iam_role_name
                )
            else:
                print(f"Warning: Could not add role to instance profile: {e}")
                raise
        
        # Wait for instance profile to be ready (AWS needs time to propagate)
        print("Waiting for IAM instance profile to be ready (AWS propagation)...")
        for i in range(6):  # Wait up to 30 seconds
            try:
                profile_info = iam.get_instance_profile(InstanceProfileName=instance_profile_name)
                if profile_info['InstanceProfile'].get('Roles'):
                    print("✓ Instance profile is ready")
                    break
            except Exception:
                pass
            if i < 5:
                time.sleep(5)
        else:
            print("⚠ Warning: Instance profile may not be fully ready, but proceeding...")
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print(f"IAM role {iam_role_name} already exists, using existing role...")
            role_response = iam.get_role(RoleName=iam_role_name)
            role_arn = role_response['Role']['Arn']
            instance_profile_name = f'{iam_role_name}-profile'
            
            # Ensure instance profile exists and has the role
            try:
                profile_info = iam.get_instance_profile(InstanceProfileName=instance_profile_name)
                roles_in_profile = [r['RoleName'] for r in profile_info['InstanceProfile'].get('Roles', [])]
                if iam_role_name not in roles_in_profile:
                    print("Adding role to existing instance profile...")
                    iam.add_role_to_instance_profile(
                        InstanceProfileName=instance_profile_name,
                        RoleName=iam_role_name
                    )
                    time.sleep(5)
            except ClientError as profile_error:
                if profile_error.response['Error']['Code'] == 'NoSuchEntity':
                    print("Creating instance profile for existing role...")
                    iam.create_instance_profile(InstanceProfileName=instance_profile_name)
                    time.sleep(2)
                    iam.add_role_to_instance_profile(
                        InstanceProfileName=instance_profile_name,
                        RoleName=iam_role_name
                    )
                    time.sleep(5)
        else:
            print(f"Error creating IAM role: {e}")
            raise
    
    # Get latest Amazon Linux 2023 AMI
    print("Finding latest Amazon Linux 2023 AMI...")
    ami_id = None
    try:
        amis = ec2.describe_images(
            Owners=['amazon'],
            Filters=[
                {'Name': 'name', 'Values': ['al2023-ami-*-x86_64']},
                {'Name': 'state', 'Values': ['available']},
                {'Name': 'architecture', 'Values': ['x86_64']}
            ]
        )
        
        if amis['Images']:
            # Sort by creation date and get the latest
            latest_ami = sorted(amis['Images'], key=lambda x: x['CreationDate'], reverse=True)[0]
            ami_id = latest_ami['ImageId']
            print(f"Using AMI: {ami_id} ({latest_ami['Name']})")
        else:
            print("Amazon Linux 2023 AMI not found, trying Amazon Linux 2...")
            raise ValueError("AL2023 not found")
    except Exception as e:
        # Fallback to Amazon Linux 2
        print("Trying Amazon Linux 2 AMI...")
        try:
            amis = ec2.describe_images(
                Owners=['amazon'],
                Filters=[
                    {'Name': 'name', 'Values': ['amzn2-ami-hvm-*-x86_64-gp2']},
                    {'Name': 'state', 'Values': ['available']},
                    {'Name': 'architecture', 'Values': ['x86_64']}
                ]
            )
            
            if amis['Images']:
                latest_ami = sorted(amis['Images'], key=lambda x: x['CreationDate'], reverse=True)[0]
                ami_id = latest_ami['ImageId']
                print(f"Using AMI: {ami_id} ({latest_ami['Name']})")
            else:
                raise ValueError("No suitable AMI found")
        except Exception as e2:
            print(f"Error: Could not find a suitable Amazon Linux AMI: {e2}")
            print("Please specify an AMI ID manually or ensure you have access to Amazon Linux AMIs.")
            raise
    
    if not ami_id:
        raise ValueError("AMI ID is required but could not be determined")
    
    # Upload script to S3 to avoid user data size limits
    # Use a fixed key so EC2 can always download the latest version
    s3_client = boto3.client('s3')
    script_bucket, script_key = get_script_s3_location()
    
    if not script_bucket:
        try:
            sts = boto3.client('sts')
            account_id = sts.get_caller_identity()['Account']
            script_bucket = f'ec2-scripts-{account_id}'
        except Exception:
            script_bucket = 'ec2-scripts-bucket'
    
    if not script_key:
        script_key = 'ec2-scripts/join.py'  # Fixed key for latest version
    
    print(f"\nUploading script to S3 (latest version)...")
    print(f"  Bucket: {script_bucket}")
    print(f"  Key: {script_key}")
    
    try:
        s3_client.put_object(
            Bucket=script_bucket,
            Key=script_key,
            Body=script_content.encode('utf-8'),
            ContentType='text/x-python'
        )
        script_s3_url = f's3://{script_bucket}/{script_key}'
        print(f"✓ Script uploaded to: {script_s3_url}")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == 'NoSuchBucket':
            # Try to create the bucket
            print(f"  Bucket doesn't exist, attempting to create it...")
            try:
                # Get region for bucket creation
                region = ec2.meta.region_name
                if region == 'us-east-1':
                    s3_client.create_bucket(Bucket=script_bucket)
                else:
                    s3_client.create_bucket(
                        Bucket=script_bucket,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                print(f"✓ Created bucket: {script_bucket}")
                # Upload script
                s3_client.put_object(
                    Bucket=script_bucket,
                    Key=script_key,
                    Body=script_content.encode('utf-8'),
                    ContentType='text/x-python'
                )
                script_s3_url = f's3://{script_bucket}/{script_key}'
                print(f"✓ Script uploaded to: {script_s3_url}")
            except Exception as create_error:
                print(f"✗ Could not create bucket: {create_error}")
                print(f"  Falling back to embedding script (may fail if too large)...")
                script_s3_url = None
        else:
            print(f"✗ Error uploading to S3: {e}")
            print(f"  Falling back to embedding script (may fail if too large)...")
            script_s3_url = None
    except Exception as e:
        print(f"✗ Unexpected error uploading to S3: {e}")
        print(f"  Falling back to embedding script (may fail if too large)...")
        script_s3_url = None
    
    # Prepare user data script
    if script_s3_url:
        # Download from S3
        user_data_script = f"""#!/bin/bash
# Update system
yum update -y

# Install Python 3 and pip
yum install -y python3 python3-pip aws-cli

# Install boto3 and dependencies
pip3 install boto3 botocore --upgrade

# Create script directory
mkdir -p /opt/data_processing

# Always download latest script from S3 (overwrites local copy)
# This ensures EC2 always runs the latest version uploaded from CloudShell
SCRIPT_BUCKET="{script_bucket}"
SCRIPT_KEY="{script_key}"
aws s3 cp s3://$SCRIPT_BUCKET/$SCRIPT_KEY /opt/data_processing/join.py && echo "✓ Script updated from S3" || echo "⚠ Using local script (S3 download failed)"
chmod +x /opt/data_processing/join.py

# Also create a wrapper that always checks for updates before running
cat > /opt/data_processing/run_always_latest.sh << 'WRAPPER_EOF'
#!/bin/bash
cd /opt/data_processing
echo "=========================================="
echo "Downloading latest script from CloudShell"
echo "=========================================="
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null)
if [ -z "$ACCOUNT_ID" ]; then
    echo "⚠ Could not get account ID, trying default bucket..."
    aws s3 cp s3://ec2-scripts-{account_id}/ec2-scripts/join.py join.py && echo "✓ Downloaded" || echo "⚠ Download failed, using local version"
else
    aws s3 cp s3://ec2-scripts-$ACCOUNT_ID/ec2-scripts/join.py join.py && echo "✓ Downloaded latest from CloudShell" || echo "⚠ Download failed, using local version"
fi
chmod +x join.py
echo "=========================================="
echo "Running script..."
echo "=========================================="
python3 join.py
WRAPPER_EOF
chmod +x /opt/data_processing/run_always_latest.sh

# Also create a simple update script
cat > /opt/data_processing/update_script.sh << 'UPDATE_EOF'
#!/bin/bash
cd /opt/data_processing
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "Downloading latest script from CloudShell..."
aws s3 cp s3://ec2-scripts-$ACCOUNT_ID/ec2-scripts/join.py join.py
chmod +x join.py
echo "✓ Script updated!"
UPDATE_EOF
chmod +x /opt/data_processing/update_script.sh

# Make script executable
chmod +x /opt/data_processing/join.py

# Install SSM agent (usually pre-installed on AL2023, but ensure it's running)
systemctl enable amazon-ssm-agent
systemctl start amazon-ssm-agent

echo "Setup complete! Script is ready at /opt/data_processing/join.py"
echo "Run with: python3 /opt/data_processing/join.py"
"""
    else:
        # Fallback: embed script (may fail if too large)
        print("⚠ Warning: Using embedded script (may exceed size limit)")
        user_data_script = f"""#!/bin/bash
# Update system
yum update -y

# Install Python 3 and pip
yum install -y python3 python3-pip

# Install boto3 and dependencies
pip3 install boto3 botocore --upgrade

# Create script directory
mkdir -p /opt/data_processing
cat > /opt/data_processing/join.py << 'SCRIPT_EOF'
{script_content}
SCRIPT_EOF

# Make script executable
chmod +x /opt/data_processing/join.py

# Install SSM agent (usually pre-installed on AL2023, but ensure it's running)
systemctl enable amazon-ssm-agent
systemctl start amazon-ssm-agent

echo "Setup complete! Script is ready at /opt/data_processing/join.py"
echo "Run with: python3 /opt/data_processing/join.py"
"""
    
    # Verify instance profile exists and is ready
    print(f"Verifying IAM instance profile: {instance_profile_name}...")
    profile_ready = False
    instance_profile_arn = None
    max_wait_attempts = 12  # Wait up to 60 seconds
    
    for attempt in range(max_wait_attempts):
        try:
            profile_response = iam.get_instance_profile(InstanceProfileName=instance_profile_name)
            profile_info = profile_response['InstanceProfile']
            
            # Check if profile has roles attached
            roles = profile_info.get('Roles', [])
            if roles:
                role_names = [r['RoleName'] for r in roles]
                if iam_role_name in role_names:
                    instance_profile_arn = profile_info['Arn']
                    print(f"✓ Instance profile verified: {instance_profile_name}")
                    print(f"  Profile ARN: {instance_profile_arn}")
                    print(f"  Role attached: {iam_role_name}")
                    profile_ready = True
                    break
                else:
                    print(f"  Waiting... Profile exists but role not attached yet (attempt {attempt + 1}/{max_wait_attempts})")
            else:
                print(f"  Waiting... Profile exists but no roles yet (attempt {attempt + 1}/{max_wait_attempts})")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchEntity':
                print(f"  Waiting... Instance profile not found yet (attempt {attempt + 1}/{max_wait_attempts})")
            else:
                print(f"  Error checking profile: {e}")
        
        if attempt < max_wait_attempts - 1:
            time.sleep(5)
    
    if not profile_ready:
        print(f"\n⚠ Warning: Instance profile may not be fully ready, but proceeding...")
        print(f"   Profile name: {instance_profile_name}")
        print(f"   If launch fails, wait a minute and try again.")
    else:
        # Additional wait for AWS propagation to EC2 service (can take 10-30 seconds)
        print("\nWaiting for instance profile to propagate to EC2 service...")
        print("(This can take 10-30 seconds after IAM profile creation)")
        time.sleep(15)  # Wait 15 seconds for propagation
    
    # Launch instance
    print(f"\nLaunching EC2 instance ({instance_type})...")
    print(f"  AMI: {ami_id}")
    print(f"  Instance Profile: {instance_profile_name}")
    if instance_profile_arn:
        print(f"  Profile ARN: {instance_profile_arn}")
    
    import base64
    # Encode user data as base64 (AWS requirement)
    user_data_encoded = base64.b64encode(user_data_script.encode('utf-8')).decode('utf-8')
    
    launch_params = {
        'ImageId': ami_id,
        'MinCount': 1,
        'MaxCount': 1,
        'InstanceType': instance_type,
        'UserData': user_data_encoded,
        'TagSpecifications': [
            {
                'ResourceType': 'instance',
                'Tags': [
                    {'Key': 'Name', 'Value': 'data-processing-join-script'},
                    {'Key': 'Purpose', 'Value': 'S3 data join processing'}
                ]
            }
        ]
    }
    
    # Try ARN first (more reliable for newly created profiles), fallback to name
    if instance_profile_arn:
        print("  Using instance profile ARN...")
        launch_params['IamInstanceProfile'] = {'Arn': instance_profile_arn}
    else:
        print("  Using instance profile name...")
        launch_params['IamInstanceProfile'] = {'Name': instance_profile_name}
    
    if key_name:
        launch_params['KeyName'] = key_name
        print(f"  Key Pair: {key_name}")
    
    if security_group_ids:
        launch_params['SecurityGroupIds'] = security_group_ids
        print(f"  Security Groups: {security_group_ids}")
    
    if subnet_id:
        launch_params['SubnetId'] = subnet_id
        print(f"  Subnet: {subnet_id}")
    
    print("\nSending launch request to EC2...")
    
    # Try launching with current method, retry with alternative if it fails
    launch_attempted = False
    for attempt in range(2):  # Try twice - once with current method, once with alternative
        try:
            if attempt == 0:
                # First attempt with current method
                pass
            else:
                # Second attempt: try alternative method
                if instance_profile_arn and launch_params['IamInstanceProfile'].get('Arn'):
                    print("\n⚠ First attempt failed. Retrying with instance profile name...")
                    launch_params['IamInstanceProfile'] = {'Name': instance_profile_name}
                elif launch_params['IamInstanceProfile'].get('Name'):
                    print("\n⚠ First attempt failed. Retrying with instance profile ARN...")
                    if instance_profile_arn:
                        launch_params['IamInstanceProfile'] = {'Arn': instance_profile_arn}
                    else:
                        # Get ARN if we don't have it
                        try:
                            profile_response = iam.get_instance_profile(InstanceProfileName=instance_profile_name)
                            instance_profile_arn = profile_response['InstanceProfile']['Arn']
                            launch_params['IamInstanceProfile'] = {'Arn': instance_profile_arn}
                        except Exception:
                            raise  # If we can't get ARN, give up
            
            response = ec2.run_instances(**launch_params)
            launch_attempted = True
            
            if 'Instances' not in response or len(response['Instances']) == 0:
                print("ERROR: No instances returned in response!")
                print(f"Response: {response}")
                raise ValueError("EC2 launch returned no instances")
            
            instance_id = response['Instances'][0]['InstanceId']
            initial_state = response['Instances'][0]['State']['Name']
            
            print(f"\n✓ Instance launch request successful!")
            print(f"  Instance ID: {instance_id}")
            print(f"  Initial State: {initial_state}")
            
            # Verify instance exists by describing it
            print("\nVerifying instance was created...")
            try:
                verify_response = ec2.describe_instances(InstanceIds=[instance_id])
                if verify_response['Reservations']:
                    verified_instance = verify_response['Reservations'][0]['Instances'][0]
                    print(f"✓ Instance verified in EC2")
                    print(f"  Current State: {verified_instance['State']['Name']}")
                    print(f"  Instance Type: {verified_instance.get('InstanceType', 'N/A')}")
                else:
                    print("⚠ Warning: Instance not found in describe_instances response")
            except Exception as e:
                print(f"⚠ Warning: Could not verify instance: {e}")
            
            print(f"\nYou can view it in the AWS Console:")
            print(f"  https://console.aws.amazon.com/ec2/v2/home?region={ec2.meta.region_name}#Instances:instanceId={instance_id}")
            break  # Success, exit retry loop
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            
            # If it's an InvalidParameterValue for instance profile and we haven't tried alternative yet
            if error_code == 'InvalidParameterValue' and 'iamInstanceProfile' in error_message.lower() and attempt == 0:
                print(f"\n⚠ Instance profile parameter issue (attempt {attempt + 1}): {error_message}")
                print("   Will retry with alternative method...")
                time.sleep(5)  # Wait a bit before retry
                continue
            else:
                # Final failure or different error
                print(f"\n✗ ERROR launching instance:")
                print(f"  Error Code: {error_code}")
                print(f"  Error Message: {error_message}")
                print(f"\nFull error details:")
                print(json.dumps(e.response, indent=2))
                raise
        except Exception as e:
            print(f"\n✗ Unexpected error launching instance: {e}")
            print(f"  Error type: {type(e).__name__}")
            import traceback
            traceback.print_exc()
            raise
    
    if not launch_attempted:
        raise RuntimeError("Failed to launch instance after retries")
    
    # Save instance details to file for later reference
    instance_info_file = 'ec2_instance_info.json'
    instance_info = {
        'instance_id': instance_id,
        'iam_role_name': iam_role_name,
        'iam_role_arn': role_arn,
        'instance_type': instance_type,
        'created_at': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
        'ssm_connect_command': f'aws ssm start-session --target {instance_id}',
        'script_location': '/opt/data_processing/join.py',
        'run_command': 'python3 /opt/data_processing/join.py'
    }
    
    try:
        with open(instance_info_file, 'w') as f:
            json.dump(instance_info, f, indent=2)
        print(f"\nInstance details saved to: {instance_info_file}")
        print("You can check status later using: python join.py --check-status")
    except Exception as e:
        print(f"Warning: Could not save instance info to file: {e}")
    
    # Wait for instance to be running
    print("\nWaiting for instance to be running (this may take 1-2 minutes)...")
    print("Note: You can check status later with: python join.py --check-status")
    waiter = ec2.get_waiter('instance_running')
    try:
        waiter.wait(InstanceIds=[instance_id], WaiterConfig={'Delay': 5, 'MaxAttempts': 60})
        print("✓ Instance is running!")
    except Exception as e:
        print(f"⚠ Warning: Instance may still be starting: {e}")
        print(f"   Instance ID: {instance_id}")
        print(f"   Check status with: aws ec2 describe-instances --instance-ids {instance_id}")
    
    # Wait for SSM to be ready (can take a few minutes)
    print("\nWaiting for SSM agent to be ready (this may take 2-5 minutes)...")
    print("The SSM agent needs to register with AWS Systems Manager before you can connect.")
    max_attempts = 40  # Increased to 40 attempts (about 6-7 minutes)
    ssm_ready = False
    for attempt in range(max_attempts):
        try:
            response = ssm.describe_instance_information(
                Filters=[
                    {
                        'Key': 'InstanceIds',
                        'Values': [instance_id]
                    }
                ]
            )
            if response['InstanceInformationList']:
                print("✓ SSM agent is ready!")
                ssm_ready = True
                break
        except Exception:
            pass
        
        if attempt < max_attempts - 1:
            elapsed_minutes = (attempt + 1) * 10 / 60
            print(f"   Waiting for SSM... ({attempt + 1}/{max_attempts} attempts, ~{elapsed_minutes:.1f} minutes elapsed)")
            time.sleep(10)
    
    if not ssm_ready:
        print("\n⚠ Warning: SSM agent may not be ready yet. This can take 5-10 minutes after instance launch.")
        print("   You can check status later with: python join.py --check-status")
        print("   Or manually check with: aws ssm describe-instance-information --filters Key=InstanceIds,Values={}".format(instance_id))
    
    # Get instance details
    instances = ec2.describe_instances(InstanceIds=[instance_id])
    instance = instances['Reservations'][0]['Instances'][0]
    public_ip = instance.get('PublicIpAddress', 'N/A')
    private_ip = instance.get('PrivateIpAddress', 'N/A')
    
    # Update instance info with IPs
    instance_info.update({
        'public_ip': public_ip,
        'private_ip': private_ip,
        'ssm_ready': ssm_ready
    })
    
    # Save updated info
    try:
        with open(instance_info_file, 'w') as f:
            json.dump(instance_info, f, indent=2)
    except Exception:
        pass
    
    result = instance_info.copy()
    
    print("\n" + "="*60)
    print("EC2 Instance Created Successfully!")
    print("="*60)
    print(f"Instance ID: {instance_id}")
    print(f"Instance Type: {instance_type}")
    print(f"Public IP: {public_ip}")
    print(f"Private IP: {private_ip}")
    print(f"SSM Ready: {'Yes' if ssm_ready else 'Not yet (check later)'}")
    print(f"\nTo connect via SSM Session Manager:")
    print(f"  aws ssm start-session --target {instance_id}")
    print(f"\nOr use the AWS Console:")
    print(f"  EC2 > Instances > {instance_id} > Connect > Session Manager")
    print(f"\nOnce connected, run the script:")
    print(f"  python3 /opt/data_processing/join.py")
    print(f"\nCheck status later with:")
    print(f"  python join.py --check-status")
    print("="*60)
    
    return result


def check_ec2_instance_status(instance_id=None):
    """
    Check the status of an EC2 instance and SSM readiness.
    If instance_id is not provided, reads from ec2_instance_info.json
    """
    ec2 = boto3.client('ec2')
    ssm = boto3.client('ssm')
    
    # If no instance_id provided, try to read from file
    if instance_id is None:
        try:
            with open('ec2_instance_info.json', 'r') as f:
                info = json.load(f)
                instance_id = info.get('instance_id')
                if not instance_id:
                    print("Error: No instance_id found in ec2_instance_info.json")
                    return
        except FileNotFoundError:
            print("Error: ec2_instance_info.json not found. Please provide instance_id or run --create-ec2 first.")
            return
        except Exception as e:
            print(f"Error reading instance info: {e}")
            return
    
    print(f"Checking status for instance: {instance_id}\n")
    
    # Check EC2 instance status
    try:
        response = ec2.describe_instances(InstanceIds=[instance_id])
        if not response['Reservations']:
            print("Error: Instance not found")
            return
        
        instance = response['Reservations'][0]['Instances'][0]
        state = instance['State']['Name']
        state_code = instance['State']['Code']
        
        print("EC2 Instance Status:")
        print(f"  State: {state} (Code: {state_code})")
        print(f"  Instance Type: {instance.get('InstanceType', 'N/A')}")
        print(f"  Public IP: {instance.get('PublicIpAddress', 'N/A')}")
        print(f"  Private IP: {instance.get('PrivateIpAddress', 'N/A')}")
        
        if state != 'running':
            print(f"\n⚠ Instance is not running yet. Current state: {state}")
            print("   Please wait for the instance to reach 'running' state before connecting.")
            return
        
    except ClientError as e:
        print(f"Error checking EC2 status: {e}")
        return
    
    # Check SSM status
    print("\nSSM Status:")
    try:
        response = ssm.describe_instance_information(
            Filters=[
                {
                    'Key': 'InstanceIds',
                    'Values': [instance_id]
                }
            ]
        )
        
        if response['InstanceInformationList']:
            ssm_info = response['InstanceInformationList'][0]
            print(f"  ✓ SSM Agent is ready!")
            print(f"  Agent Version: {ssm_info.get('AgentVersion', 'N/A')}")
            print(f"  Ping Status: {ssm_info.get('PingStatus', 'N/A')}")
            print(f"  Last Ping: {ssm_info.get('LastPingDateTime', 'N/A')}")
            print(f"\n✓ Ready to connect!")
            print(f"  Command: aws ssm start-session --target {instance_id}")
        else:
            print("  ⚠ SSM agent not registered yet.")
            print("     This can take 5-10 minutes after instance launch.")
            print("     The instance needs to:")
            print("     1. Finish booting")
            print("     2. Install/start SSM agent")
            print("     3. Register with Systems Manager")
            print("\n  Check again in a few minutes.")
            
    except Exception as e:
        print(f"  ⚠ Error checking SSM status: {e}")
        print("     SSM may not be ready yet.")


def upload_script_to_s3():
    """
    Upload the current script to S3 so EC2 instances can download the latest version.
    """
    try:
        s3 = boto3.client('s3')
        script_bucket, script_key = get_script_s3_location()
        
        if not script_bucket or not script_key:
            print("Error: Could not determine S3 location")
            return False
        
        # Read current script file
        try:
            with open(__file__, 'r') as f:
                script_content = f.read()
        except Exception as e:
            print(f"Error reading script file: {e}")
            return False
        
        print(f"Uploading script to S3...")
        print(f"  Bucket: {script_bucket}")
        print(f"  Key: {script_key}")
        
        try:
            s3.put_object(
                Bucket=script_bucket,
                Key=script_key,
                Body=script_content.encode('utf-8'),
                ContentType='text/x-python'
            )
            print(f"✓ Script uploaded successfully!")
            print(f"  EC2 instances will download this version on next run")
            print(f"  Location: s3://{script_bucket}/{script_key}")
            return True
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == 'NoSuchBucket':
                print(f"Error: Bucket '{script_bucket}' does not exist")
                print(f"  Create it first or run --create-ec2 to set it up")
            else:
                print(f"Error uploading: {e}")
            return False
    except Exception as e:
        print(f"Error: {e}")
        return False


if __name__ == "__main__":
    # CRITICAL: If running on EC2, ALWAYS download latest from CloudShell (S3) first
    # This happens BEFORE anything else - even argument parsing
    import os
    import sys
    
    script_path = __file__
    is_ec2 = '/opt/data_processing' in script_path
    
    if is_ec2:
        # We're on EC2 - ALWAYS download latest from S3 (uploaded from CloudShell) before running
        print("="*70)
        print("EC2 Instance - Getting latest script from CloudShell (via S3)")
        print("="*70)
        print("Step 1: Downloading latest version from S3 (uploaded by CloudShell)...")
        
        try:
            import boto3
            from botocore.exceptions import ClientError
            
            s3 = boto3.client('s3')
            sts = boto3.client('sts')
            
            # Get account ID and S3 location (where CloudShell uploads)
            account_id = sts.get_caller_identity()['Account']
            script_bucket = f'ec2-scripts-{account_id}'
            script_key = 'ec2-scripts/join.py'
            
            print(f"  S3 Location: s3://{script_bucket}/{script_key}")
            
            try:
                # Step 1: Download latest version from S3 (uploaded from CloudShell)
                s3.download_file(script_bucket, script_key, script_path)
                print("  ✓ Downloaded latest script from CloudShell!")
                print("="*70)
                print("Step 2: Restarting with new version...")
                print("="*70)
                
                # Step 2: Restart with the new version (which will then run)
                os.execv(sys.executable, [sys.executable] + sys.argv)
                # This line never executes due to execv
                
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                if error_code in ['NoSuchKey', '404']:
                    print("  ⚠ Script not found in S3.")
                    print("     Run 'python join.py --create-ec2' from CloudShell first.")
                elif error_code == 'NoSuchBucket':
                    print("  ⚠ S3 bucket doesn't exist.")
                    print("     Run 'python join.py --create-ec2' from CloudShell first.")
                else:
                    print(f"  ⚠ Error downloading: {error_code}")
                print("  Using current script version...")
                print("="*70)
            except Exception as e:
                print(f"  ⚠ Error downloading: {e}")
                print("  Using current script version...")
                print("="*70)
                
        except Exception as e:
            print(f"  ⚠ Could not connect to S3: {e}")
            print("  Using current script version...")
            print("="*70)
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Data processing script with EC2 deployment option')
    parser.add_argument('--create-ec2', action='store_true', 
                       help='Create an EC2 instance to run this script')
    parser.add_argument('--upload-script', action='store_true',
                       help='Upload current script to S3 for EC2 instances to use')
    parser.add_argument('--check-status', action='store_true',
                       help='Check the status of an existing EC2 instance')
    parser.add_argument('--instance-id', 
                       help='Instance ID to check (if not using saved instance info)')
    parser.add_argument('--instance-type', default='m5.4xlarge',
                       help='EC2 instance type (default: m5.4xlarge)')
    parser.add_argument('--subnet-id', help='Subnet ID for the instance')
    parser.add_argument('--security-group-ids', nargs='+',
                       help='Security group IDs (space-separated)')
    
    args = parser.parse_args()
    
    # If running from CloudShell (not EC2), always upload to S3 first
    # This ensures EC2 always has the latest version available
    if not is_ec2:
        try:
            import boto3
            from botocore.exceptions import ClientError
            
            s3 = boto3.client('s3')
            sts = boto3.client('sts')
            
            account_id = sts.get_caller_identity()['Account']
            script_bucket = f'ec2-scripts-{account_id}'
            script_key = 'ec2-scripts/join.py'
            
            print("CloudShell detected - Uploading script to S3...")
            print(f"  Location: s3://{script_bucket}/{script_key}")
            
            try:
                with open(__file__, 'r') as f:
                    script_content = f.read()
                
                try:
                    s3.put_object(
                        Bucket=script_bucket,
                        Key=script_key,
                        Body=script_content.encode('utf-8'),
                        ContentType='text/x-python'
                    )
                    print(f"✓ Script uploaded to S3")
                except ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchBucket':
                        # Create bucket
                        region = boto3.Session().region_name or 'us-east-1'
                        if region == 'us-east-1':
                            s3.create_bucket(Bucket=script_bucket)
                        else:
                            s3.create_bucket(
                                Bucket=script_bucket,
                                CreateBucketConfiguration={'LocationConstraint': region}
                            )
                        s3.put_object(
                            Bucket=script_bucket,
                            Key=script_key,
                            Body=script_content.encode('utf-8'),
                            ContentType='text/x-python'
                        )
                        print(f"✓ Created bucket and uploaded script to S3")
            except Exception as e:
                print(f"⚠ Could not upload to S3: {e}")
        except Exception:
            pass  # Continue even if upload fails
    
    if args.create_ec2:
        create_ec2_instance_for_processing(
            instance_type=args.instance_type,
            subnet_id=args.subnet_id,
            security_group_ids=args.security_group_ids
        )
    elif args.upload_script:
        upload_script_to_s3()
    elif args.check_status:
        check_ec2_instance_status(instance_id=args.instance_id)
    else:
        stream_and_match()