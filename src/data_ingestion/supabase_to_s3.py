# src/data_ingestion/supabase_to_s3.py
"""
Export pipeline: Supabase PostgreSQL -> AWS S3 Raw bucket (V2).

Incremental export with Hive-style partitioning and checkpoint tracking.

S3 structure:
  supabase_exports/
  ├── ingestion_date=YYYY-MM-DD/
  │   └── jobs_export_{timestamp}.csv
  └── checkpoint/
      └── last_export.txt

Usage:
    python src/data_ingestion/supabase_to_s3.py

Environment variables:
    DATABASE_URL   - Supabase PostgreSQL connection string
    AWS_REGION     - AWS region (default: us-east-1)
    S3_RAW_BUCKET  - Target S3 bucket (default: hirenovik-raw-dev)
"""

import logging
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("supabase_to_s3")

S3_RAW_BUCKET = os.getenv("S3_RAW_BUCKET", "hirenovik-raw-dev")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
CHECKPOINT_KEY = "supabase_exports/checkpoint/last_export.txt"

EXPORT_QUERY = text("""
    SELECT
        id,
        site,
        job_url,
        title,
        company,
        raw_location,
        country,
        country_code,
        state,
        career,
        scraped_by,
        industry_niche,
        job_type,
        salary_min,
        salary_max,
        currency,
        description,
        date_posted,
        date_scraped,
        status,
        CASE
            WHEN salary_min IS NOT NULL
            AND salary_max IS NOT NULL
            THEN true
            ELSE false
        END as has_salary
    FROM jobs
    WHERE status IN ('NEW', 'PENDING_ENRICHMENT')
    ORDER BY date_scraped DESC
""")


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


def get_database_engine():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")
    database_url = database_url.replace("postgres://", "postgresql://")
    return create_engine(
        database_url,
        pool_size=2,
        max_overflow=0,
        pool_pre_ping=True,
    )


def extract_from_supabase(engine, checkpoint_date=None) -> pd.DataFrame:
    logger.info("Connecting to Supabase and running export query")
    try:
        with engine.connect() as conn:
            if checkpoint_date:
                logger.info("Incremental mode: exporting records after %s", checkpoint_date)
                query = EXPORT_QUERY.text + " AND date_scraped > :checkpoint_date"
                df = pd.read_sql(
                    text(query),
                    conn,
                    params={"checkpoint_date": checkpoint_date},
                )
            else:
                logger.info("Full mode: exporting all records")
                df = pd.read_sql(EXPORT_QUERY, conn)
        logger.info("Extraction complete. rows=%d columns=%d", len(df), len(df.columns))
        return df
    except SQLAlchemyError as exc:
        logger.error("Database extraction failed: %s", exc)
        raise


# ---------------------------------------------------------------------------
# S3 Checkpoint
# ---------------------------------------------------------------------------


def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def read_checkpoint(bucket: str, s3_client) -> datetime | None:
    logger.info("Reading checkpoint from s3://%s/%s", bucket, CHECKPOINT_KEY)
    try:
        response = s3_client.get_object(Bucket=bucket, Key=CHECKPOINT_KEY)
        content = response["Body"].read().decode("utf-8").strip()
        if not content:
            logger.warning("Checkpoint file is empty")
            return None
        dt = datetime.fromisoformat(content)
        logger.info("Checkpoint found: %s", dt)
        return dt
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            logger.info("No checkpoint file found. This is normal on first run.")
            return None
        logger.error("Failed to read checkpoint: %s", exc)
        return None


def write_checkpoint(bucket: str, s3_client, dt: datetime) -> None:
    content = dt.isoformat()
    logger.info("Writing checkpoint: %s -> s3://%s/%s", content, bucket, CHECKPOINT_KEY)
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=CHECKPOINT_KEY,
            Body=content.encode("utf-8"),
            ContentType="text/plain",
        )
        logger.info("Checkpoint written successfully")
    except (BotoCoreError, ClientError) as exc:
        logger.warning("Failed to write checkpoint: %s", exc)
        raise


# ---------------------------------------------------------------------------
# Partitioning & Upload
# ---------------------------------------------------------------------------


def group_by_ingestion_date(df: pd.DataFrame) -> dict:
    """Split DataFrame into Hive partitions by the date part of date_scraped."""
    groups = {}
    for _, row in df.iterrows():
        if pd.notna(row["date_scraped"]):
            date_key = row["date_scraped"].strftime("%Y-%m-%d")
        else:
            date_key = "unknown"
        if date_key not in groups:
            groups[date_key] = []
        groups[date_key].append(row)
    result = {}
    for date_key, rows in groups.items():
        result[date_key] = pd.DataFrame(rows)
    logger.info("Grouped into %d partition(s): %s", len(result), list(result.keys()))
    return result


def write_csv_to_temp(df: pd.DataFrame, filename: str) -> Path:
    tmp_dir = Path(tempfile.gettempdir())
    filepath = tmp_dir / filename
    df.to_csv(filepath, index=False, encoding="utf-8")
    size_mb = filepath.stat().st_size / (1024 * 1024)
    logger.info("CSV written to temp file. path=%s size_mb=%.2f", filepath, size_mb)
    return filepath


def upload_to_s3(local_path: Path, bucket: str, s3_key: str) -> None:
    s3_client = get_s3_client()
    logger.info("Uploading to S3. bucket=%s key=%s", bucket, s3_key)
    try:
        s3_client.upload_file(
            str(local_path),
            bucket,
            s3_key,
            ExtraArgs={"ServerSideEncryption": "AES256"},
        )
        logger.info("Upload complete. s3_uri=s3://%s/%s", bucket, s3_key)
    except (BotoCoreError, ClientError) as exc:
        logger.error("S3 upload failed: %s", exc)
        raise


def verify_upload(bucket: str, s3_key: str) -> int:
    s3_client = get_s3_client()
    try:
        response = s3_client.head_object(Bucket=bucket, Key=s3_key)
        size_bytes = response["ContentLength"]
        logger.info(
            "Upload verified. s3_uri=s3://%s/%s size_bytes=%d",
            bucket,
            s3_key,
            size_bytes,
        )
        return size_bytes
    except ClientError as exc:
        logger.error("Upload verification failed: %s", exc)
        raise


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run_export() -> dict:
    bucket = S3_RAW_BUCKET
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    logger.info("Starting Supabase to S3 export. bucket=%s", bucket)

    s3_client = get_s3_client()

    # 1. Read checkpoint to determine incremental vs full export
    checkpoint_date = read_checkpoint(bucket, s3_client)

    # 2. Extract from Supabase
    engine = get_database_engine()
    df = extract_from_supabase(engine, checkpoint_date)

    if df.empty:
        logger.warning("No records returned from Supabase. Aborting export.")
        return {"records_exported": 0, "s3_uri": None}

    # 3. Group by ingestion_date for Hive-compatible partitions
    partitions = group_by_ingestion_date(df)

    # 4. Upload each partition
    uploaded_uris = []
    for date_key, partition_df in partitions.items():
        filename = f"jobs_export_{timestamp}.csv"
        s3_key = f"supabase_exports/ingestion_date={date_key}/{filename}"

        local_path = write_csv_to_temp(partition_df, filename)
        upload_to_s3(local_path, bucket, s3_key)
        verify_upload(bucket, s3_key)
        local_path.unlink()
        logger.info("Temp file cleaned up. path=%s", local_path)

        uploaded_uris.append(f"s3://{bucket}/{s3_key}")

    # 5. Update checkpoint with the newest date_scraped from this batch
    max_date = df["date_scraped"].max()
    if pd.notna(max_date):
        if max_date.tzinfo is None:
            max_date = max_date.replace(tzinfo=timezone.utc)
        write_checkpoint(bucket, s3_client, max_date)

    result = {
        "records_exported": len(df),
        "columns": len(df.columns),
        "partitions": len(partitions),
        "files": uploaded_uris,
        "checkpoint_updated": max_date.isoformat() if pd.notna(max_date) else None,
        "timestamp": timestamp,
    }

    logger.info(
        "Export pipeline complete. records=%d partitions=%d checkpoint=%s",
        result["records_exported"],
        result["partitions"],
        result["checkpoint_updated"],
    )

    return result


if __name__ == "__main__":
    try:
        result = run_export()
        if result["records_exported"] == 0:
            sys.exit(0)
    except Exception as exc:
        logger.critical("Export pipeline failed with unhandled exception: %s", exc)
        sys.exit(1)
