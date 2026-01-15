#!/usr/bin/env python3
"""
Fetch nucleotideAlignment and siloReads file IDs for each submissionId from the WASAP API.

Usage: python fetch_file_ids.py <output_file> [api_url]

Arguments:
    output_file: Path to write the JSON output (e.g., file_ids.json)
    api_url: Optional API URL (default: https://api.db.wasap.genspectrum.org/backend/rsva/get-released-data)

Output format (JSON):
    {
        "submissionId1": {
            "nucleotideAlignment": "file_id_or_null",
            "siloReads": "file_id_or_null"
        },
        ...
    }
"""

import json
import sys
import urllib.request

DEFAULT_API_URL = "https://api.db.wasap.genspectrum.org/backend/rsva/get-released-data"


def fetch_released_data(api_url: str) -> list[dict]:
    """Fetch released data from the API (NDJSON format)."""
    request = urllib.request.Request(
        api_url,
        headers={"Accept": "application/x-ndjson"}
    )

    records = []
    with urllib.request.urlopen(request) as response:
        for line in response:
            line = line.decode('utf-8').strip()
            if line:
                records.append(json.loads(line))

    return records


def parse_file_field(field_value: str | None) -> list[dict]:
    """Parse a JSON string field containing file info array."""
    if not field_value:
        return []
    try:
        return json.loads(field_value)
    except (json.JSONDecodeError, TypeError):
        return []


def extract_file_ids(records: list[dict]) -> dict:
    """Extract nucleotideAlignment and siloReads file IDs per submissionId.

    The API returns data nested in 'metadata', and file fields are JSON strings
    containing arrays of {fileId, name, url} objects.
    """
    result = {}

    for record in records:
        # Data is nested inside 'metadata'
        metadata = record.get("metadata", {})
        submission_id = metadata.get("submissionId")
        if not submission_id:
            continue

        # Parse the JSON string fields to extract fileIds
        alignment_files = parse_file_field(metadata.get("nucleotideAlignment"))
        silo_files = parse_file_field(metadata.get("siloReads"))

        result[submission_id] = {
            "nucleotideAlignment": [f.get("fileId") for f in alignment_files if f.get("fileId")],
            "siloReads": [f.get("fileId") for f in silo_files if f.get("fileId")]
        }

    return result


def main():
    if len(sys.argv) < 2:
        print("Usage: python fetch_file_ids.py <output_file> [api_url]")
        print("Example: python fetch_file_ids.py file_ids.json")
        sys.exit(1)

    output_file = sys.argv[1]
    api_url = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_API_URL

    print(f"Fetching data from: {api_url}")
    records = fetch_released_data(api_url)
    print(f"Fetched {len(records)} records")

    file_ids = extract_file_ids(records)
    print(f"Extracted file IDs for {len(file_ids)} submissions")

    # Count file IDs (now lists)
    alignment_count = sum(len(v["nucleotideAlignment"]) for v in file_ids.values())
    silo_count = sum(len(v["siloReads"]) for v in file_ids.values())
    print(f"  - nucleotideAlignment: {alignment_count} files")
    print(f"  - siloReads: {silo_count} files")

    with open(output_file, 'w') as f:
        json.dump(file_ids, f, indent=2)

    print(f"Written to: {output_file}")


if __name__ == "__main__":
    main()
