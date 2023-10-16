"""Windowed aggregations for KPIs and analytics."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, window, count, countDistinct, sum as spark_sum, avg, 
    when, expr, struct, collect_list, desc, row_number
)
from pyspark.sql.window import Window


def setup_windowed_aggregations(df: DataFrame) -> DataFrame:
    """Compute windowed KPIs, funnels, and top products."""
    
    # 1-minute tumbling windows for main KPIs
    kpi_aggregates = df.groupBy(
        window(col("event_ts"), "1 minute").alias("window")
    ).agg(
        # Session metrics
        countDistinct("session_id").alias("sessions"),
        
        # Event counts by type
        count(when(col("event_type") == "page_view", 1)).alias("page_views"),
        count(when(col("event_type") == "add_to_cart", 1)).alias("add_to_carts"),
        count(when(col("event_type") == "purchase", 1)).alias("purchases"),
        
        # Revenue metrics
        spark_sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("revenue"),
        
        # Latency metrics
        expr("percentile_approx(latency_ms, 0.95)").alias("latency_ms_p95")
    ).withColumn(
        "minute", col("window.start")
    ).withColumn(
        "conversion_rate", 
        when(col("sessions") > 0, col("purchases") / col("sessions")).otherwise(0)
    ).withColumn(
        "aov",
        when(col("purchases") > 0, col("revenue") / col("purchases")).otherwise(0)
    ).select(
        col("minute"),
        col("sessions"),
        col("page_views"), 
        col("add_to_carts"),
        col("purchases"),
        col("conversion_rate"),
        col("revenue"),
        col("aov"),
        col("latency_ms_p95")
    )
    
    return kpi_aggregates

# Updated: 2025-10-04 19:46:27
# Added during commit replay


# Updated: 2025-10-04 19:46:33
# Added during commit replay
