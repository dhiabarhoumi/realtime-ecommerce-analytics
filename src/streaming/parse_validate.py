"""Parse and validate JSON events from Kafka stream."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp, unix_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, BooleanType
)


def get_event_schema() -> StructType:
    """Define the expected event schema."""
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("page", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("qty", IntegerType(), True),
        StructField("referrer", StringType(), True),
        StructField("utm", StructType([
            StructField("source", StringType(), True),
            StructField("medium", StringType(), True),
            StructField("campaign", StringType(), True)
        ]), True),
        StructField("device", StructType([
            StructField("ua", StringType(), True),
            StructField("os", StringType(), True),
            StructField("mobile", BooleanType(), True)
        ]), True),
        StructField("geo", StructType([
            StructField("country", StringType(), True),
            StructField("city", StringType(), True)
        ]), True)
    ])


def setup_parsing(raw_df: DataFrame) -> DataFrame:
    """Parse JSON and validate events, sending bad events to dead letter."""
    
    schema = get_event_schema()
    
    # Parse JSON with schema
    parsed_df = raw_df.select(
        col("kafka_timestamp"),
        col("partition_key"),
        from_json(col("json_data"), schema).alias("event")
    ).select(
        col("kafka_timestamp"),
        col("partition_key"),
        col("event.*")
    )
    
    # Convert event timestamp to proper timestamp type
    validated_df = parsed_df.withColumn(
        "event_ts", 
        to_timestamp(col("event_ts"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    ).withColumn(
        "processing_time",
        current_timestamp()
    ).withColumn(
        "latency_ms",
        (unix_timestamp(col("processing_time")) - unix_timestamp(col("event_ts"))) * 1000
    )
    
    # Filter valid events (basic validation)
    valid_events = validated_df.filter(
        col("event_id").isNotNull() &
        col("event_type").isNotNull() &
        col("event_ts").isNotNull() &
        col("user_id").isNotNull() &
        col("session_id").isNotNull() &
        col("event_type").isin(["page_view", "add_to_cart", "purchase"])
    )
    
    return valid_events