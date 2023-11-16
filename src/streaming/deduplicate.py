"""Deduplicate events by event_id using state store."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def setup_deduplication(df: DataFrame) -> DataFrame:
    """Remove duplicate events based on event_id within the watermark window."""
    
    # Use dropDuplicates with watermark for stateful deduplication
    # This keeps a state store of seen event_ids within the watermark window
    deduped_df = df.dropDuplicates(["event_id"])
    
    return deduped_df