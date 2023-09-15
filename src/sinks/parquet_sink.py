"""Parquet sink for cold storage of aggregated data."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import year, month, dayofmonth, hour


def setup_parquet_sink(df: DataFrame, output_path: str):
    """Write DataFrame to partitioned Parquet for cold storage."""
    
    try:
        # Add partitioning columns
        partitioned_df = df.withColumn("year", year("minute")) \
                          .withColumn("month", month("minute")) \
                          .withColumn("day", dayofmonth("minute")) \
                          .withColumn("hour", hour("minute"))
        
        # Write to Parquet with partitioning
        partitioned_df.write \
            .mode("append") \
            .partitionBy("year", "month", "day", "hour") \
            .parquet(output_path)
        
        print(f"✓ Appended {df.count()} records to Parquet: {output_path}")
        
    except Exception as e:
        print(f"❌ Parquet write error: {e}")
        raise

# Updated: 2025-10-04 19:46:27
# Added during commit replay
