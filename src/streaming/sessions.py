"""Session window computation for user activity."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, session_window


def setup_sessionization(df: DataFrame) -> DataFrame:
    """Add session windows based on user activity with 30-minute timeout."""
    
    # Add session window based on user_id and event_ts
    # 30-minute gap threshold for session timeout
    session_df = df.withColumn(
        "session_window",
        session_window(col("event_ts"), "30 minutes")
    )
    
    return session_df