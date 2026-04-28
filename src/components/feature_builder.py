from pyspark.sql.functions import (
    window,
    count,
    sum as spark_sum,
    when,
    col,
    countDistinct,
    lag,
    avg,
)
from pyspark.sql.window import Window


def build_window_features(df, window_size="1 minute"):
    """
    Builds time-window based aggregated features for anomaly detection.

    Features:
    - event_count
    - warn_count
    - unique_components
    - unique_processes
    - warn_ratio
    - event_delta
    - rolling_mean
    - warn_intensity

    Args:
        df (DataFrame): Clean log DataFrame with 'datetime', 'log_level', etc.
        window_size (str): Spark window duration (default: 1 minute)

    Returns:
        DataFrame: Aggregated feature DataFrame
    """

    # ============================================================
    # 🔥 STEP 1: Window Aggregation
    # ============================================================
    features_df = (
        df.groupBy(window(col("datetime"), window_size).alias("w"))
        .agg(
            count("*").alias("event_count"),

            spark_sum(
                when(col("log_level") == "WARN", 1).otherwise(0)
            ).alias("warn_count"),

            countDistinct("component").alias("unique_components"),
            countDistinct("process_id").alias("unique_processes"),
        )
    )

    # ============================================================
    # 🔥 STEP 2: Flatten Window Columns
    # ============================================================
    features_df = features_df.select(
        col("w.start").alias("bucket_start"),
        col("w.end").alias("bucket_end"),
        "event_count",
        "warn_count",
        "unique_components",
        "unique_processes",
    )

    # ============================================================
    # 🔥 STEP 3: Handle Edge Cases (Division Safety)
    # ============================================================
    features_df = features_df.withColumn(
        "warn_ratio",
        when(col("event_count") > 0,
            col("warn_count") / col("event_count")
        ).otherwise(0)
    )

    window_spec = Window.orderBy("bucket_start")

    features_df = features_df.withColumn(
        "event_delta",
        col("event_count") - lag("event_count", 1).over(window_spec)
    )

    features_df = features_df.withColumn(
        "rolling_mean",
        avg("event_count").over(window_spec.rowsBetween(-3, 0))
    )

    features_df = features_df.withColumn(
        "warn_intensity",
        col("warn_count") / (col("unique_processes") + 1)
    )

    features_df = features_df.fillna(0)

    # ============================================================
    # 🔥 STEP 4: Sort for Temporal Consistency
    # ============================================================
    features_df = features_df.orderBy("bucket_start")

    return features_df
