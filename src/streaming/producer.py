#!/usr/bin/env python
import os
import sys
import time
import argparse
import gzip
import json
import requests
from datetime import datetime, timedelta
from confluent_kafka import Producer

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        # Do not print every message to prevent log flooding, just log periodically or on error
        pass

def get_gharchive_url(date_str, hour):
    """
    Format: https://data.gharchive.org/YYYY-MM-DD-H.json.gz
    Note: Hour is 0-23 without leading zeros.
    """
    return f"https://data.gharchive.org/{date_str}-{hour}.json.gz"

def stream_to_kafka(url, producer, topic, delay):
    print(f"Streaming from URL: {url} to topic: {topic}")
    try:
        response = requests.get(url, stream=True, timeout=30)
        if response.status_code == 404:
            print(f"File not found (404): {url}. It might not be generated yet.")
            return False
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to fetch data from GH Archive: {e}")
        return False

    count = 0
    start_time = time.time()
    try:
        # Decompress on the fly
        with gzip.GzipFile(fileobj=response.raw) as gfile:
            for line in gfile:
                # Decode line and load JSON to validate
                event_str = line.decode('utf-8').strip()
                if not event_str:
                    continue
                
                try:
                    event = json.loads(event_str)
                    event_id = event.get('id')
                    
                    # Produce message to Kafka
                    producer.produce(
                        topic=topic,
                        key=str(event_id) if event_id else None,
                        value=event_str,
                        callback=delivery_report
                    )
                    
                    # Serve delivery callbacks
                    producer.poll(0)
                    
                    count += 1
                    if count % 1000 == 0:
                        print(f"Produced {count} events...")
                        
                    if delay > 0:
                        time.sleep(delay)
                        
                except json.JSONDecodeError:
                    print("Error decoding JSON line, skipping...")
                    continue
                except Exception as e:
                    print(f"Error producing event to Kafka: {e}")
                    
        # Flush any outstanding messages in queue
        print("Flushing remaining messages in producer queue...")
        producer.flush()
        duration = time.time() - start_time
        print(f"Successfully streamed {count} events in {duration:.2f} seconds.")
        return True
        
    except Exception as e:
        print(f"Error reading and streaming gzip content: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="GitPulse Live Streaming Producer")
    parser.add_argument("--date", type=str, help="Date in YYYY-MM-DD format (default: 2 hours ago)")
    parser.add_argument("--hour", type=int, help="Hour between 0 and 23 (default: 2 hours ago)")
    parser.add_argument("--delay", type=float, default=0.001, help="Delay between messages in seconds (default: 1ms)")
    args = parser.parse_args()

    # Determine default date/hour (2 hours ago to ensure file exists on GH Archive)
    if not args.date or args.hour is None:
        target_time = datetime.utcnow() - timedelta(hours=2)
        default_date = target_time.strftime("%Y-%m-%d")
        default_hour = target_time.hour
        
        args.date = args.date or default_date
        args.hour = args.hour if args.hour is not None else default_hour

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("TOPIC_NAME", "github.events.raw")
    
    print(f"Starting producer: bootstrap_servers={bootstrap_servers}, topic={topic}")
    
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'gitpulse-producer',
        'queue.buffering.max.messages': 500000,
        'linger.ms': 100,
        'batch.num.messages': 1000
    }
    
    try:
        producer = Producer(conf)
    except Exception as e:
        print(f"Failed to create Kafka producer: {e}")
        sys.exit(1)
        
    url = get_gharchive_url(args.date, args.hour)
    success = stream_to_kafka(url, producer, topic, args.delay)
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()
