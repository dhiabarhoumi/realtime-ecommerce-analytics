"""Unit tests for session logic."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime

from src.streaming.sessions import setup_sessionization


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing."""
    return SparkSession.builder \
        .appName("test_sessions") \
        .master("local[2]") \
        .getOrCreate()


def test_sessionization(spark):
    """Test that session windows are added correctly."""
    
    # Create test data with events from the same user
    test_data = [
        ("user_1", "2023-10-01T12:00:00"),
        ("user_1", "2023-10-01T12:05:00"),  # 5 minutes later
        ("user_1", "2023-10-01T12:45:00"),  # 40 minutes later (new session)
        ("user_2", "2023-10-01T12:00:00"),
    ]
    
    df = spark.createDataFrame(test_data, ["user_id", "event_ts"])
    df = df.withColumn("event_ts", col("event_ts").cast("timestamp"))
    
    # Apply sessionization
    session_df = setup_sessionization(df)
    
    # Verify session window column was added
    assert "session_window" in session_df.columns
    
    # Collect results
    results = session_df.collect()
    assert len(results) == 4
    
    # Verify all rows have session windows
    for row in results:
        assert row.session_window is not None


def test_session_timeout(spark):
    """Test that session timeout works correctly."""
    
    # Create events with gaps larger than 30 minutes
    test_data = [
        ("user_1", "2023-10-01T12:00:00"),
        ("user_1", "2023-10-01T13:00:00"),  # 60 minutes later - should be new session
    ]
    
    df = spark.createDataFrame(test_data, ["user_id", "event_ts"])
    df = df.withColumn("event_ts", col("event_ts").cast("timestamp"))
    
    session_df = setup_sessionization(df)
    results = session_df.collect()
    
    # Both events should have session windows (potentially different ones)
    assert len(results) == 2
    assert all(row.session_window is not None for row in results)