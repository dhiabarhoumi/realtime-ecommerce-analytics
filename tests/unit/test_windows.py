"""Unit tests for windowed aggregations."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from datetime import datetime

from src.streaming.windows import setup_windowed_aggregations


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing."""
    return SparkSession.builder \
        .appName("test_windows") \
        .master("local[2]") \
        .getOrCreate()


def test_windowed_aggregations(spark):
    """Test windowed KPI calculations."""
    
    # Create test events within the same minute window
    base_time = "2023-10-01T12:00:"
    test_data = [
        ("session_1", "page_view", f"{base_time}10", None, 0),
        ("session_1", "add_to_cart", f"{base_time}20", 29.99, 100),
        ("session_1", "purchase", f"{base_time}30", 29.99, 50),
        ("session_2", "page_view", f"{base_time}40", None, 200),
        ("session_2", "purchase", f"{base_time}50", 49.99, 75),
    ]
    
    df = spark.createDataFrame(test_data, ["session_id", "event_type", "event_ts", "price", "latency_ms"])
    df = df.withColumn("event_ts", col("event_ts").cast("timestamp"))
    
    # Apply windowed aggregations
    agg_df = setup_windowed_aggregations(df)
    
    # Verify expected columns
    expected_cols = [
        "minute", "sessions", "page_views", "add_to_carts", 
        "purchases", "conversion_rate", "revenue", "aov", "latency_ms_p95"
    ]
    
    for col_name in expected_cols:
        assert col_name in agg_df.columns
    
    # Collect and verify results
    results = agg_df.collect()
    assert len(results) == 1  # One minute window
    
    row = results[0]
    assert row.sessions == 2  # 2 unique sessions
    assert row.page_views == 2
    assert row.add_to_carts == 1
    assert row.purchases == 2
    assert row.revenue == 79.98  # 29.99 + 49.99
    assert row.conversion_rate == 1.0  # 2 purchases / 2 sessions


def test_empty_aggregations(spark):
    """Test aggregations with no data."""
    
    # Empty DataFrame with required schema
    schema = "session_id string, event_type string, event_ts timestamp, price double, latency_ms int"
    df = spark.createDataFrame([], schema)
    
    agg_df = setup_windowed_aggregations(df)
    
    # Should return empty result
    assert agg_df.count() == 0


def test_conversion_rate_calculation(spark):
    """Test conversion rate calculation edge cases."""
    
    # Test case with sessions but no purchases
    test_data = [
        ("session_1", "page_view", "2023-10-01T12:00:10", None, 100),
        ("session_2", "page_view", "2023-10-01T12:00:20", None, 150),
    ]
    
    df = spark.createDataFrame(test_data, ["session_id", "event_type", "event_ts", "price", "latency_ms"])
    df = df.withColumn("event_ts", col("event_ts").cast("timestamp"))
    
    agg_df = setup_windowed_aggregations(df)
    results = agg_df.collect()
    
    assert len(results) == 1
    row = results[0]
    assert row.sessions == 2
    assert row.purchases == 0
    assert row.conversion_rate == 0.0

# Updated: 2025-10-04 19:46:29
# Added during commit replay


# Updated: 2025-10-04 19:46:35
# Added during commit replay
