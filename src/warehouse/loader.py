#!/usr/bin/env python
import os
import sys
import argparse
import io
import boto3
import pandas as pd
import psycopg2
from psycopg2 import extras
import pyarrow.parquet as pq

def get_s3_client():
    endpoint_url = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    
    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=boto3.session.Config(signature_version='s3v4')
    )

def get_db_connection():
    conn_str = os.getenv("PG_CONN_STR", "postgresql://postgres:postgres@localhost:5432/gitpulse")
    return psycopg2.connect(conn_str)

def load_partition(year, month, day, hour):
    s3_client = get_s3_client()
    bucket = os.getenv("BUCKET_NAME", "gitpulse-lake")
    prefix = f"year={year:04d}/month={month:02d}/day={day:02d}/hour={hour:02d}/"
    
    print(f"Checking for files in MinIO: s3://{bucket}/{prefix}")
    
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    except Exception as e:
        print(f"Error listing S3 bucket objects: {e}")
        return False
        
    if 'Contents' not in response:
        print(f"No parquet files found for partition {prefix}. Skipping.")
        return True
        
    keys = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
    print(f"Found {len(keys)} parquet files to load.")
    
    if not keys:
        return True

    # Establish db connection
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return False

    total_rows = 0
    total_inserted = 0

    try:
        for key in keys:
            print(f"Downloading and reading {key}...")
            obj_resp = s3_client.get_object(Bucket=bucket, Key=key)
            parquet_data = obj_resp['Body'].read()
            
            # Read Parquet file from bytes using PyArrow
            table = pq.read_table(io.BytesIO(parquet_data))
            df = table.to_pandas()
            
            rows_count = len(df)
            total_rows += rows_count
            if rows_count == 0:
                continue
                
            print(f"Loaded {rows_count} rows from Parquet file. Inserting into temp table...")
            
            # Create a unique temporary staging table name
            temp_table = f"temp_staging_{uuid_hash()}"
            
            # Create temp table matching structure of raw.github_events but without constraints
            cursor.execute(f"""
                CREATE TEMP TABLE {temp_table} (
                    id BIGINT,
                    type VARCHAR(50),
                    actor TEXT,
                    repo TEXT,
                    org TEXT,
                    payload TEXT,
                    public BOOLEAN,
                    created_at VARCHAR(30)
                ) ON COMMIT DROP;
            """)
            
            # Format DataFrame for bulk insert
            # Select columns to match the temp table
            df_insert = df[['id', 'type', 'actor', 'repo', 'org', 'payload', 'public', 'created_at']]
            
            # Replace NaNs or Nones with None
            df_insert = df_insert.where(pd.notnull(df_insert), None)
            
            tuples = [tuple(x) for x in df_insert.to_numpy()]
            
            # Bulk copy to temp table
            insert_query = f"""
                INSERT INTO {temp_table} (id, type, actor, repo, org, payload, public, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            extras.execute_batch(cursor, insert_query, tuples, page_size=2000)
            
            # Idempotent MERGE/Upsert query from temp table to target table
            cursor.execute(f"""
                INSERT INTO raw.github_events (id, type, actor, repo, org, payload, public, created_at)
                SELECT id, type, actor, repo, org, payload, public, created_at
                FROM {temp_table}
                ON CONFLICT (id) DO NOTHING;
            """)
            
            inserted = cursor.rowcount
            total_inserted += inserted
            print(f"Upserted {inserted} new records from {key}.")
            
        conn.commit()
        print(f"Partition load completed. Total processed: {total_rows}, Total inserted: {total_inserted}")
        return True
        
    except Exception as e:
        print(f"Database error during loading: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def uuid_hash():
    import uuid
    return uuid.uuid4().hex[:8]

def main():
    parser = argparse.ArgumentParser(description="GitPulse MinIO to Postgres Idempotent Loader")
    parser.add_argument("--year", type=int, help="Partition Year (YYYY)")
    parser.add_argument("--month", type=int, help="Partition Month (MM)")
    parser.add_argument("--day", type=int, help="Partition Day (DD)")
    parser.add_argument("--hour", type=int, help="Partition Hour (HH)")
    args = parser.parse_args()

    # Default to current hour if partitions are omitted
    from datetime import datetime, timedelta
    now = datetime.utcnow()
    year = args.year if args.year is not None else now.year
    month = args.month if args.month is not None else now.month
    day = args.day if args.day is not None else now.day
    hour = args.hour if args.hour is not None else now.hour

    success = load_partition(year, month, day, hour)
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()
