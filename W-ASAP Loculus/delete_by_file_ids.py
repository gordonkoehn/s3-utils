#!/usr/bin/env python3
"""
Safely delete nucleotideAlignment and siloReads files from S3 by file ID.

This script reads a JSON file (produced by fetch_file_ids.py) and deletes the
corresponding files from an S3-compatible bucket.

Usage: python delete_by_file_ids.py <file_ids_json> <bucket_name> <prefix> <access_key> <secret_key> <endpoint> <region> [--dry-run] [--type TYPE]

Arguments:
    file_ids_json: Path to JSON file with file IDs (from fetch_file_ids.py)
    bucket_name: S3 bucket name
    prefix: Prefix path in bucket (e.g., 'data/' or '')
    access_key: S3 access key
    secret_key: S3 secret key
    endpoint: S3 endpoint URL
    region: S3 region

Options:
    --dry-run: Preview deletions without actually deleting
    --type TYPE: Only delete files of specified type (nucleotideAlignment or siloReads)

Safety features:
    - Dry-run mode to preview what would be deleted
    - Confirmation prompt before actual deletion
    - Detailed logging of each operation
    - Summary report at the end
"""

import boto3
import json
import sys


def parse_args(argv):
    """Parse command-line arguments."""
    args = {
        "dry_run": False,
        "file_type": None,  # None means both types
    }

    # Filter out options
    positional = []
    i = 0
    while i < len(argv):
        if argv[i] == "--dry-run":
            args["dry_run"] = True
        elif argv[i] == "--type":
            if i + 1 < len(argv):
                args["file_type"] = argv[i + 1]
                i += 1
            else:
                print("Error: --type requires a value (nucleotideAlignment or siloReads)")
                sys.exit(1)
        else:
            positional.append(argv[i])
        i += 1

    return positional, args


def load_file_ids(json_path: str) -> dict:
    """Load file IDs from JSON file."""
    with open(json_path, 'r') as f:
        return json.load(f)


def collect_files_to_delete(file_ids: dict, prefix: str, file_type: str | None) -> list[tuple[str, str, str]]:
    """
    Collect list of files to delete.

    Returns: List of (submission_id, file_type, s3_key) tuples

    Note: file_ids values contain lists of file IDs (not single values)
    """
    files = []

    for submission_id, ids in file_ids.items():
        if file_type is None or file_type == "nucleotideAlignment":
            for file_id in ids.get("nucleotideAlignment", []):
                s3_key = f"{prefix}{file_id}" if prefix else file_id
                files.append((submission_id, "nucleotideAlignment", s3_key))

        if file_type is None or file_type == "siloReads":
            for file_id in ids.get("siloReads", []):
                s3_key = f"{prefix}{file_id}" if prefix else file_id
                files.append((submission_id, "siloReads", s3_key))

    return files


def main():
    positional, options = parse_args(sys.argv[1:])

    if len(positional) < 7:
        print("Usage: python delete_by_file_ids.py <file_ids_json> <bucket_name> <prefix> <access_key> <secret_key> <endpoint> <region> [--dry-run] [--type TYPE]")
        print("")
        print("Options:")
        print("  --dry-run          Preview deletions without actually deleting")
        print("  --type TYPE        Only delete 'nucleotideAlignment' or 'siloReads'")
        sys.exit(1)

    file_ids_json = positional[0]
    bucket_name = positional[1]
    prefix = positional[2]
    access_key = positional[3]
    secret_key = positional[4]
    endpoint = positional[5]
    region = positional[6]

    # Ensure endpoint has https:// prefix
    if not endpoint.startswith("http://") and not endpoint.startswith("https://"):
        endpoint = f"https://{endpoint}"

    dry_run = options["dry_run"]
    file_type = options["file_type"]

    # Validate file_type if specified
    if file_type and file_type not in ("nucleotideAlignment", "siloReads"):
        print(f"Error: --type must be 'nucleotideAlignment' or 'siloReads', got '{file_type}'")
        sys.exit(1)

    # Load file IDs
    print(f"Loading file IDs from: {file_ids_json}")
    file_ids = load_file_ids(file_ids_json)
    print(f"Loaded {len(file_ids)} submissions")

    # Collect files to delete
    files_to_delete = collect_files_to_delete(file_ids, prefix, file_type)
    print(f"Found {len(files_to_delete)} files to delete")

    if not files_to_delete:
        print("No files to delete.")
        return

    # Show preview
    print("\nFiles to delete:")
    print("-" * 80)
    for submission_id, ftype, s3_key in files_to_delete[:10]:
        print(f"  [{ftype}] {s3_key} (submission: {submission_id})")
    if len(files_to_delete) > 10:
        print(f"  ... and {len(files_to_delete) - 10} more files")
    print("-" * 80)

    if dry_run:
        print("\n[DRY RUN] No files were deleted.")
        print(f"Would delete {len(files_to_delete)} files from s3://{bucket_name}/")
        return

    # Confirmation prompt
    print(f"\nWARNING: This will permanently delete {len(files_to_delete)} files from s3://{bucket_name}/")
    confirmation = input("Type 'DELETE' to confirm: ")
    if confirmation != "DELETE":
        print("Aborted.")
        sys.exit(0)

    # Initialize S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint,
        region_name=region
    )

    # Delete files
    deleted_count = 0
    error_count = 0

    for submission_id, ftype, s3_key in files_to_delete:
        try:
            s3.delete_object(Bucket=bucket_name, Key=s3_key)
            print(f"Deleted: {s3_key}")
            deleted_count += 1
        except Exception as e:
            print(f"Error deleting {s3_key}: {e}")
            error_count += 1

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Successfully deleted: {deleted_count}")
    print(f"Errors: {error_count}")
    print(f"Total processed: {len(files_to_delete)}")


if __name__ == "__main__":
    main()
