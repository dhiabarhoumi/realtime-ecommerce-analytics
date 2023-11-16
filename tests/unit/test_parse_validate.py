"""Unit tests for parsing and validation logic."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import json

from src.streaming.parse_validate import get_event_schema, setup_parsing


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing."""
    return SparkSession.builder \
        .appName("test") \
        .master("local[2]") \
        .getOrCreate()


def test_event_schema():
    """Test that event schema is properly defined."""
    schema = get_event_schema()
    assert isinstance(schema, StructType)
    
    field_names = [field.name for field in schema.fields]
    expected_fields = [
        "event_id", "event_type", "event_ts", "user_id", "session_id",
        "page", "product_id", "price", "currency", "qty", "referrer",
        "utm", "device", "geo"
    ]
    
    for field in expected_fields:
        assert field in field_names


def test_parse_valid_event(spark):
    """Test parsing of valid event JSON."""
    
    # Sample valid event
    valid_event = {
        "event_id": "test-123",
        "event_type": "page_view",
        "event_ts": "2023-10-01T12:00:00.000Z",
        "user_id": "user_123",
        "session_id": "session_123",
        "page": "/product/sku_001",
        "product_id": "sku_001",
        "price": 29.99,
        "currency": "USD",
        "qty": 1,
        "referrer": "https://google.com",
        "utm": {"source": "google", "medium": "cpc", "campaign": "test"},
        "device": {"ua": "Mozilla/5.0", "os": "Windows", "mobile": False},
        "geo": {"country": "US", "city": "New York"}
    }
    
    # Create test DataFrame
    test_data = [(json.dumps(valid_event), "session_123")]
    df = spark.createDataFrame(test_data, ["json_data", "partition_key"])
    df = df.withColumn("kafka_timestamp", df.partition_key)  # Mock timestamp
    
    # Parse events
    parsed_df = setup_parsing(df)
    
    # Verify parsing
    assert parsed_df.count() == 1
    
    row = parsed_df.collect()[0]
    assert row.event_id == "test-123"
    assert row.event_type == "page_view"
    assert row.user_id == "user_123"


def test_parse_invalid_event(spark):
    """Test that invalid events are filtered out."""
    
    # Invalid event (missing required fields)
    invalid_event = {
        "event_id": "test-456",
        # Missing event_type, user_id, etc.
        "page": "/test"
    }
    
    # Create test DataFrame
    test_data = [(json.dumps(invalid_event), "session_456")]
    df = spark.createDataFrame(test_data, ["json_data", "partition_key"])
    df = df.withColumn("kafka_timestamp", df.partition_key)
    
    # Parse events
    parsed_df = setup_parsing(df)
    
    # Should be filtered out
    assert parsed_df.count() == 0


def test_parse_malformed_json(spark):
    """Test handling of malformed JSON."""
    
    # Malformed JSON
    malformed_json = '{"event_id": "test", "incomplete":'
    
    # Create test DataFrame
    test_data = [(malformed_json, "session_malformed")]
    df = spark.createDataFrame(test_data, ["json_data", "partition_key"])
    df = df.withColumn("kafka_timestamp", df.partition_key)
    
    # Should not crash and return empty result
    parsed_df = setup_parsing(df)
    assert parsed_df.count() == 0