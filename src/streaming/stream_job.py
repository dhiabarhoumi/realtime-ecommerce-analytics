#!/usr/bin/env python3
"""Main Spark Structured Streaming job for e-commerce analytics."""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from parse_validate import setup_parsing
from deduplicate import setup_deduplication
from sessions import setup_sessionization
from windows import setup_windowed_aggregations
from sinks.mongo_upsert import setup_mongo_sink
from sinks.parquet_sink import setup_parquet_sink


def create_spark_session(app_name: str = "EcommerceAnalytics") -> SparkSession:
    """Create Spark session with required configurations."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.streaming.checkpointLocation.deleteOnExit", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


def main():
    parser = argparse.ArgumentParser(description="E-commerce real-time analytics")
    parser.add_argument("--brokers", required=True, help="Kafka brokers")
    parser.add_argument("--mongo_uri", required=True, help="MongoDB URI")
    parser.add_argument("--mongo_db", required=True, help="MongoDB database")
    parser.add_argument("--mongo_col", required=True, help="MongoDB collection")
    parser.add_argument("--checkpoint", required=True, help="Checkpoint location")
    parser.add_argument("--out_parquet", required=True, help="Parquet output path")
    parser.add_argument("--starting_offsets", default="latest", help="Kafka starting offsets")
    parser.add_argument("--watermark", default="10 minutes", help="Watermark interval")
    parser.add_argument("--trigger_interval", default="10 seconds", help="Processing trigger interval")
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"Starting streaming job...")
    print(f"Kafka brokers: {args.brokers}")
    print(f"MongoDB: {args.mongo_uri}/{args.mongo_db}.{args.mongo_col}")
    print(f"Checkpoint: {args.checkpoint}")
    print(f"Parquet output: {args.out_parquet}")
    
    try:
        # Read from Kafka
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", args.brokers) \
            .option("subscribe", "events_raw") \
            .option("startingOffsets", args.starting_offsets) \
            .option("failOnDataLoss", "false") \
            .load()
        
        print("✓ Connected to Kafka")
        
        # Extract Kafka metadata and parse JSON
        raw_events = kafka_df.selectExpr(
            "CAST(value as STRING) as json_data",
            "CAST(key as STRING) as partition_key", 
            "topic",
            "partition",
            "offset",
            "timestamp as kafka_timestamp"
        )
        
        # Parse and validate events
        parsed_events = setup_parsing(raw_events)
        print("✓ Setup event parsing and validation")
        
        # Add watermark for event time processing
        watermarked_events = parsed_events.withWatermark("event_ts", args.watermark)
        print(f"✓ Added watermark: {args.watermark}")
        
        # Deduplicate by event_id
        deduped_events = setup_deduplication(watermarked_events)
        print("✓ Setup deduplication")
        
        # Add session windows
        session_events = setup_sessionization(deduped_events)
        print("✓ Setup sessionization")
        
        # Compute windowed aggregations
        kpi_aggregates = setup_windowed_aggregations(session_events)
        print("✓ Setup windowed aggregations")
        
        # Setup sinks
        def write_to_sinks(batch_df, batch_id):
            """Write batch to both MongoDB and Parquet."""
            print(f"Processing batch {batch_id} with {batch_df.count()} records")
            
            if batch_df.count() > 0:
                # Write to MongoDB (upsert)
                setup_mongo_sink(batch_df, args.mongo_uri, args.mongo_db, args.mongo_col)
                
                # Write to Parquet (append)
                setup_parquet_sink(batch_df, args.out_parquet)
                
                print(f"✓ Batch {batch_id} written to sinks")
        
        # Start the streaming query
        query = kpi_aggregates.writeStream \
            .foreachBatch(write_to_sinks) \
            .option("checkpointLocation", args.checkpoint) \
            .trigger(processingTime=args.trigger_interval) \
            .outputMode("append") \
            .start()
        
        print("✓ Streaming query started")
        print(f"Dashboard: http://localhost:8501")
        print("Press Ctrl+C to stop...")
        
        # Wait for termination
        query.awaitTermination()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()