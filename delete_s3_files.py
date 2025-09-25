#!/usr/bin/env python3
"""
Script to delete all files in an S3-compatible bucket under a specified prefix.
Usage: python delete_s3_files.py <bucket_name> <prefix> <access_key> <secret_key> <endpoint> <region>
"""

import boto3
import sys

if len(sys.argv) < 7:
    print("Usage: python delete_s3_files.py <bucket_name> <prefix> <access_key> <secret_key> <endpoint> <region>")
    sys.exit(1)

bucket_name = sys.argv[1]
prefix = sys.argv[2]
access_key = sys.argv[3]
secret_key = sys.argv[4]
endpoint = sys.argv[5]
region = sys.argv[6]

# Initialize S3 client with provided credentials and custom endpoint
s3 = boto3.client('s3', 
                  aws_access_key_id=access_key, 
                  aws_secret_access_key=secret_key, 
                  endpoint_url=endpoint, 
                  region_name=region)

# Use paginator to handle large number of objects
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

deleted_count = 0
for page in pages:
    if 'Contents' in page:
        for obj in page['Contents']:
            print(f"Deleting: {obj['Key']}")
            s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
            deleted_count += 1

print(f"Deleted {deleted_count} objects from {bucket_name}/{prefix}")