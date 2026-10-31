"""
Batch extractor: pull season files and land them in object storage, unchanged.

The landing zone is deliberately dumb. Files arrive exactly as the source sent
them, under a key that records the season and the extraction date. Nothing is
parsed, renamed or cleaned here, so that a bad transform can always be replayed
from the original bytes instead of re-fetched from a source that may have
changed underneath us.

Locally this copies from data/landing into MinIO. Pointed at a real feed it
fetches over HTTP; set SOURCE_BASE_URL and pass --remote.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path


def s3_client():
    try:
        import boto3
    except ImportError:
        sys.exit("boto3 is not installed; run `pip install boto3`")

    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "minioadmin"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def ensure_bucket(client, bucket: str) -> None:
    existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)
        print(f"created bucket {bucket}")


def fetch_remote(season: str, dataset: str) -> bytes:
    import urllib.request

    base = os.getenv("SOURCE_BASE_URL")
    if not base:
        sys.exit("SOURCE_BASE_URL is not set; cannot run --remote")

    url = f"{base.rstrip('/')}/{dataset}/{season}.csv"
    print(f"GET {url}")
    with urllib.request.urlopen(url, timeout=60) as resp:
        if resp.status != 200:
            raise RuntimeError(f"{url} returned {resp.status}")
        return resp.read()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--seasons", nargs="*", default=["2023-24", "2024-25", "2025-26"])
    ap.add_argument("--datasets", nargs="*", default=["matches", "events"])
    ap.add_argument("--bucket", default=os.getenv("LAKE_BUCKET", "matchlake"))
    ap.add_argument("--local-root", default="data/landing")
    ap.add_argument("--remote", action="store_true", help="Fetch from SOURCE_BASE_URL instead of the local root")
    args = ap.parse_args()

    client = s3_client()
    ensure_bucket(client, args.bucket)
    run_date = date.today().isoformat()
    uploaded = 0

    for dataset in args.datasets:
        for season in args.seasons:
            key = f"landing/{dataset}/season={season}/extracted_date={run_date}/{dataset}.csv"

            if args.remote:
                body = fetch_remote(season, dataset)
            else:
                path = Path(args.local_root) / dataset / f"season={season}" / f"{dataset}.csv"
                if not path.exists():
                    print(f"[skip] {path} not found", file=sys.stderr)
                    continue
                body = path.read_bytes()

            client.put_object(Bucket=args.bucket, Key=key, Body=body)
            print(f"[ok] s3://{args.bucket}/{key} ({len(body):,} bytes)")
            uploaded += 1

    # Land the slowly changing reference data too, unpartitioned.
    teams = Path(args.local_root) / "teams" / "teams.csv"
    if teams.exists():
        client.put_object(
            Bucket=args.bucket, Key=f"landing/teams/extracted_date={run_date}/teams.csv", Body=teams.read_bytes()
        )
        uploaded += 1

    if uploaded == 0:
        sys.exit("nothing was uploaded; failing so the orchestrator does not mark this green")
    print(f"landed {uploaded} objects")


if __name__ == "__main__":
    main()
