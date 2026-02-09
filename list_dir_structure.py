import boto3

def session_s3_tree(bucket_name, prefix=''):
    """List S3 keys under bucket/prefix and print as a tree. bucket_name must be just the bucket (e.g. 'kindred-0'); prefix is the key prefix (e.g. 'MX2/')."""
    if '/' in bucket_name and not prefix:
        # Allow single argument like 'kindred-0/MX2/' -> bucket='kindred-0', prefix='MX2/'
        bucket_name, prefix = bucket_name.split('/', 1)
        if prefix and not prefix.endswith('/'):
            prefix = prefix + '/'
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get('Contents', []):
            keys.append(obj['Key'])

    # Collect only folder paths (every prefix; no file names)
    folder_paths = set()
    for key in keys:
        parts = key.rstrip('/').split('/')
        # Add each prefix path (everything before the last segment, which is the file)
        for i in range(1, len(parts)):
            folder_paths.add('/'.join(parts[:i]))

    for path in sorted(folder_paths):
        depth = path.count('/')
        name = path.split('/')[-1]
        print('    ' * depth + '|-- ' + name)

# Usage: pass bucket and prefix separately, or as one path
session_s3_tree('kindred-0/MX2/') 