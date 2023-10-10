#!/usr/bin/env python3
"""Replay producer that reads events from CSV and streams to Kafka at configurable rate."""

import argparse
import gzip
import json
import time
from datetime import datetime
from typing import Iterator

import pandas as pd
from kafka import KafkaProducer


class ReplayProducer:
    def __init__(self, brokers: str = "localhost:9092", topic: str = "events_raw"):
        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None,
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=10,
        )
        self.topic = topic
        self.events_sent = 0
        self.start_time = None

    def load_events(self, file_path: str) -> pd.DataFrame:
        """Load events from compressed CSV file."""
        print(f"Loading events from {file_path}...")
        
        if file_path.endswith('.gz'):
            df = pd.read_csv(file_path, compression='gzip')
        else:
            df = pd.read_csv(file_path)
        
        # Convert string columns back to proper format
        df['utm'] = df['utm'].apply(lambda x: eval(x) if pd.notna(x) else {})
        df['device'] = df['device'].apply(lambda x: eval(x) if pd.notna(x) else {})
        df['geo'] = df['geo'].apply(lambda x: eval(x) if pd.notna(x) else {})
        
        print(f"Loaded {len(df)} events")
        return df

    def events_iterator(self, df: pd.DataFrame, rate_eps: int) -> Iterator[dict]:
        """Iterate through events at specified rate (events per second)."""
        if rate_eps <= 0:
            # Send all events as fast as possible
            for _, row in df.iterrows():
                yield row.to_dict()
        else:
            # Rate-limited sending
            interval = 1.0 / rate_eps
            
            for _, row in df.iterrows():
                if self.start_time is None:
                    self.start_time = time.time()
                
                # Calculate when this event should be sent
                target_time = self.start_time + (self.events_sent * interval)
                current_time = time.time()
                
                # Sleep if we're ahead of schedule
                if current_time < target_time:
                    time.sleep(target_time - current_time)
                
                yield row.to_dict()

    def clean_event(self, event: dict) -> dict:
        """Clean event data for JSON serialization."""
        cleaned = {}
        for key, value in event.items():
            if pd.isna(value):
                cleaned[key] = None
            elif isinstance(value, (int, float, str, bool)):
                cleaned[key] = value
            else:
                # Handle dict columns that may be stored as strings
                if isinstance(value, str) and value.startswith('{'):
                    try:
                        cleaned[key] = eval(value)
                    except:
                        cleaned[key] = {}
                else:
                    cleaned[key] = value
        
        # Update timestamp to current time for realistic streaming
        cleaned['event_ts'] = datetime.utcnow().isoformat() + 'Z'
        
        return cleaned

    def produce_events(self, file_path: str, rate_eps: int = 500, max_events: int = None):
        """Produce events from file at specified rate."""
        df = self.load_events(file_path)
        
        if max_events:
            df = df.head(max_events)
            print(f"Limited to first {max_events} events")
        
        print(f"Starting replay at {rate_eps} events/second...")
        self.start_time = time.time()
        
        try:
            for event_data in self.events_iterator(df, rate_eps):
                # Clean and send event
                event = self.clean_event(event_data)
                
                # Use session_id as partition key for session affinity
                partition_key = event.get('session_id', '')
                
                self.producer.send(
                    self.topic,
                    value=event,
                    key=partition_key
                )
                
                self.events_sent += 1
                
                # Print progress every 1000 events
                if self.events_sent % 1000 == 0:
                    elapsed = time.time() - self.start_time
                    current_rate = self.events_sent / elapsed if elapsed > 0 else 0
                    print(f"Sent {self.events_sent} events | Rate: {current_rate:.1f} eps | "
                          f"Target: {rate_eps} eps")
                
        except KeyboardInterrupt:
            print(f"\\nStopped after sending {self.events_sent} events")
        
        finally:
            print("Flushing remaining events...")
            self.producer.flush()
            self.producer.close()
            
            if self.start_time:
                elapsed = time.time() - self.start_time
                avg_rate = self.events_sent / elapsed if elapsed > 0 else 0
                print(f"\\nFinal stats:")
                print(f"Events sent: {self.events_sent}")
                print(f"Duration: {elapsed:.1f}s")
                print(f"Average rate: {avg_rate:.1f} eps")


def main():
    parser = argparse.ArgumentParser(description="Replay e-commerce events to Kafka")
    parser.add_argument("--file", required=True, help="Path to events CSV file")
    parser.add_argument("--brokers", default="localhost:9092", help="Kafka brokers")
    parser.add_argument("--topic", default="events_raw", help="Kafka topic")
    parser.add_argument("--rate", type=int, default=500, help="Events per second (0 = unlimited)")
    parser.add_argument("--max-events", type=int, help="Maximum events to send")
    
    args = parser.parse_args()
    
    producer = ReplayProducer(brokers=args.brokers, topic=args.topic)
    producer.produce_events(args.file, args.rate, args.max_events)


if __name__ == "__main__":
    main()

# Updated: 2025-10-04 19:46:32
# Added during commit replay
