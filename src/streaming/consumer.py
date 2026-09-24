#!/usr/bin/env python
import os
import sys
import io
import time
import json
import uuid
import boto3
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException

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

def parse_event_time(created_at_str):
    """
    Parse created_at from event.
    Standard GHA formats: '2026-08-11T23:38:11Z' or '2026-08-11 23:38:11'
    """
    try:
        # Strip Z if present
        clean_str = created_at_str.replace('Z', '')
        if 'T' in clean_str:
            dt = datetime.strptime(clean_str, "%Y-%m-%dT%H:%M:%S")
        else:
            dt = datetime.strptime(clean_str, "%Y-%m-%d %H:%M:%S")
        return dt
    except Exception as e:
        # Fallback to current time
        return datetime.utcnow()

def serialize_field(field):
    if field is None:
        return None
    if isinstance(field, (dict, list)):
        return json.dumps(field)
    return str(field)

def write_buffer_to_s3(s3_client, bucket, key_prefix, events):
    """
    Convert list of events to PyArrow Table and write as Parquet to S3/MinIO
    """
    if not events:
        return True
        
    schema = pa.schema([
        ('id', pa.int64()),
        ('type', pa.string()),
        ('actor', pa.string()),
        ('repo', pa.string()),
        ('org', pa.string()),
        ('payload', pa.string()),
        ('public', pa.bool_()),
        ('created_at', pa.string())
    ])
    
    data = {
        'id': [],
        'type': [],
        'actor': [],
        'repo': [],
        'org': [],
        'payload': [],
        'public': [],
        'created_at': []
    }
    
    for event in events:
        try:
            data['id'].append(int(event.get('id')))
        except (ValueError, TypeError):
            # Generate a hash if ID is missing or invalid
            data['id'].append(hash(event.get('created_at', '')) & 0xffffffff)
            
        data['type'].append(str(event.get('type', 'UnknownEvent')))
        data['actor'].append(serialize_field(event.get('actor')))
        data['repo'].append(serialize_field(event.get('repo')))
        data['org'].append(serialize_field(event.get('org')))
        data['payload'].append(serialize_field(event.get('payload')))
        data['public'].append(bool(event.get('public', True)))
        data['created_at'].append(str(event.get('created_at')))
        
    try:
        table = pa.Table.from_pydict(data, schema=schema)
        parquet_buffer = io.BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_buffer.seek(0)
        
        file_name = f"events_{uuid.uuid4().hex}.parquet"
        s3_key = f"{key_prefix}/{file_name}"
        
        print(f"Uploading {len(events)} events to s3://{bucket}/{s3_key}")
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=parquet_buffer.getvalue()
        )
        return True
    except Exception as e:
        print(f"Failed to write parquet to S3: {e}")
        return False

def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("TOPIC_NAME", "github.events.raw")
    bucket = os.getenv("BUCKET_NAME", "gitpulse-lake")
    
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'gitpulse-consumer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    
    try:
        consumer = Consumer(conf)
        consumer.subscribe([topic])
        print(f"Consumer started: subscribed to {topic}")
    except Exception as e:
        print(f"Failed to start Kafka consumer: {e}")
        sys.exit(1)
        
    s3_client = get_s3_client()
    
    # In-memory buffer: partition_key -> [list of parsed events]
    buffers = {}
    # Track the last write time for each partition key
    last_write_time = {}
    
    # Configuration limits
    BATCH_SIZE_LIMIT = 5000  # Number of rows before flushing
    TIME_LIMIT_SEC = 30     # Flush every 30 seconds
    
    print("Entering consume loop...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                # Check if we should flush any partition buffers based on time limit
                now = time.time()
                for part_key, buf in list(buffers.items()):
                    if buf and (now - last_write_time.get(part_key, 0)) >= TIME_LIMIT_SEC:
                        print(f"Time limit reached for {part_key}, flushing...")
                        success = write_buffer_to_s3(s3_client, bucket, part_key, buf)
                        if success:
                            buffers[part_key] = []
                            last_write_time[part_key] = now
                            consumer.commit(asynchronous=False)
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Kafka error: {msg.error()}")
                    raise KafkaException(msg.error())
                    
            # Parse message value
            try:
                event_str = msg.value().decode('utf-8')
                event = json.loads(event_str)
            except Exception as e:
                print(f"Error parsing message value: {e}")
                continue
                
            created_at = event.get('created_at')
            if not created_at:
                created_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                event['created_at'] = created_at
                
            dt = parse_event_time(created_at)
            
            # Format partition key: year=YYYY/month=MM/day=DD/hour=HH
            part_key = f"year={dt.year:04d}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}"
            
            if part_key not in buffers:
                buffers[part_key] = []
                last_write_time[part_key] = time.time()
                
            buffers[part_key].append(event)
            
            # If batch limit reached, flush this partition
            if len(buffers[part_key]) >= BATCH_SIZE_LIMIT:
                print(f"Batch size limit reached for {part_key}, flushing...")
                success = write_buffer_to_s3(s3_client, bucket, part_key, buffers[part_key])
                if success:
                    buffers[part_key] = []
                    last_write_time[part_key] = time.time()
                    consumer.commit(asynchronous=False)
                    
    except KeyboardInterrupt:
        print("Consume loop interrupted by user.")
    finally:
        # Flush any remaining buffers on shutdown
        print("Flushing remaining buffers on shutdown...")
        for part_key, buf in buffers.items():
            if buf:
                write_buffer_to_s3(s3_client, bucket, part_key, buf)
        consumer.close()
        print("Consumer closed.")

if __name__ == "__main__":
    main()
