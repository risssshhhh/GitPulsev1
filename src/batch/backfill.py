#!/usr/bin/env python
import os
import sys
import argparse
import requests
import shutil
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, year, month, dayofmonth, hour, coalesce, to_timestamp

def download_file(url, local_path):
    print(f"Downloading {url} to {local_path}...")
    try:
        response = requests.get(url, stream=True, timeout=30)
        if response.status_code == 404:
            print(f"File not found (404): {url}")
            return False
        response.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        return True
    except Exception as e:
        print(f"Error downloading file: {e}")
        return False

def get_date_hours_list(start_date_str, end_date_str):
    """
    Generate list of (date_str, hour) tuples between start and end date (inclusive)
    """
    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d") + timedelta(days=1)
    
    date_hours = []
    current_dt = start_dt
    while current_dt < end_dt:
        date_str = current_dt.strftime("%Y-%m-%d")
        for hour in range(24):
            date_hours.append((date_str, hour))
        current_dt += timedelta(days=1)
        
    return date_hours

def main():
    parser = argparse.ArgumentParser(description="GitPulse PySpark Historical Backfill")
    parser.add_argument("--start-date", type=str, required=True, help="Start Date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, required=True, help="End Date (YYYY-MM-DD)")
    parser.add_argument("--hour", type=int, help="Hour to backfill (0-23)")
    parser.add_argument("--temp-dir", type=str, default="/tmp/gitpulse_spark", help="Temp dir for downloads")
    args = parser.parse_args()

    # Environment configs
    s3_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    bucket = os.getenv("BUCKET_NAME", "gitpulse-lake")

    print(f"Initializing PySpark backfill: {args.start_date} to {args.end_date} hour {args.hour}")
    
    # 1. Download target files
    os.makedirs(args.temp_dir, exist_ok=True)
    date_hours = get_date_hours_list(args.start_date, args.end_date)
    if args.hour is not None:
        date_hours = [dh for dh in date_hours if dh[1] == args.hour]

    
    downloaded_files = []
    for date_str, hr in date_hours:
        url = f"https://data.gharchive.org/{date_str}-{hr}.json.gz"
        local_filename = f"{date_str}-{hr}.json.gz"
        local_path = os.path.join(args.temp_dir, local_filename)
        
        # Download file if it doesn't already exist locally
        if not os.path.exists(local_path):
            success = download_file(url, local_path)
            if success:
                downloaded_files.append(local_path)
        else:
            print(f"File already exists locally: {local_path}")
            downloaded_files.append(local_path)
            
    if not downloaded_files:
        print("No files were downloaded. Exiting.")
        sys.exit(0)

    # 2. Setup Spark Session with S3A Hadoop package config
    # We use org.apache.hadoop:hadoop-aws:3.3.4 package to write to S3
    spark = SparkSession.builder \
        .appName("GitPulseBackfill") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

    print("Spark Session initialized successfully.")
    
    try:
        # 3. Read raw JSON gzip files
        print(f"Reading {len(downloaded_files)} files from {args.temp_dir} into Spark DataFrame...")
        df = spark.read.json(os.path.join(args.temp_dir, "*.json.gz"))
        
        print("Schema parsed by Spark:")
        df.printSchema()
        
        # 4. Transform and align columns with the streaming consumer schema
        ts_col = coalesce(
            to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss")
        )
        
        df_processed = df.select(
            col("id").cast("long").alias("id"),
            col("type"),
            to_json(col("actor")).alias("actor"),
            to_json(col("repo")).alias("repo"),
            to_json(col("org")).alias("org"),
            to_json(col("payload")).alias("payload"),
            col("public").cast("boolean").alias("public"),
            col("created_at"),
            # Partition fields
            year(ts_col).alias("year"),
            month(ts_col).alias("month"),
            dayofmonth(ts_col).alias("day"),
            hour(ts_col).alias("hour")
        ).filter(col("id").isNotNull())
        
        # 5. Write to MinIO partitioned
        output_s3_path = f"s3a://{bucket}/"
        print(f"Writing partitioned Parquet datasets to {output_s3_path}...")
        
        df_processed.write \
            .mode("append") \
            .partitionBy("year", "month", "day", "hour") \
            .parquet(output_s3_path)
            
        print("PySpark backfill job finished successfully.")
        
    except Exception as e:
        print(f"Error executing Spark backfill job: {e}")
        sys.exit(1)
    finally:
        # Clean up temporary downloads
        print("Cleaning up temporary local files...")
        try:
            shutil.rmtree(args.temp_dir)
        except Exception as e:
            print(f"Error cleaning up temp dir: {e}")
        spark.stop()

if __name__ == "__main__":
    main()
