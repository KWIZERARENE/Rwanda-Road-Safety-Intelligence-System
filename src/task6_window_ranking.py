"""
Task 6: Advanced Location Ranking with Window Functions (RRSIS Project)
-----------------------------------------------------------------------
- Calculates severity risk measure per location grouped by geographical category (Urban vs Rural Area).
- Applies PySpark Window functions (row_number, rank, dense_rank) over partitions.
- Demonstrates:
  - Window specification creation: Window.partitionBy().orderBy()
  - Window ranking functions: row_number(), rank(), dense_rank()
  - Multi-condition boolean filtering with &
  - Column calculation & renaming using select(..., col().alias())
  - Analytical comparison of ranking behavior
- Extracts and presents the Top 3 highest-risk locations within each geographical category.

Run standalone:
    python src/task6_window_ranking.py
"""

import os
import sys

# Ensure repository root and src directory are in sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    from src.spark_session import get_spark_session
    from src.task4_severity_index import run as run_task4
except ImportError:
    from spark_session import get_spark_session
    from task4_severity_index import run as run_task4


def run(spark=None, df_sev=None):
    own_spark = spark is None
    if own_spark:
        spark = get_spark_session("RRSIS_Task6_WindowRanking")

    if df_sev is None:
        df_sev = run_task4(spark)

    print("\n==========================================================")
    print(" TASK 6: ADVANCED LOCATION RANKING VIA SPARK WINDOW FUNCTIONS")
    print("==========================================================")

    # Aggregate location metrics partitioned by Urban_or_Rural_Area and Local_Authority_District
    location_category_agg = (
        df_sev.groupBy("Urban_or_Rural_Area", "Local_Authority_District")
        .agg(
            F.count("*").alias("Total_Accidents"),
            F.sum("Severity_Weight").alias("Severity_Score"),
            F.sum(F.when(F.col("Accident_Severity") == "Fatal", 1).otherwise(0)).alias("Fatal_Count"),
            F.sum(F.when(F.col("Accident_Severity") == "Serious", 1).otherwise(0)).alias("Serious_Count"),
            F.round(F.avg("Severity_Weight"), 2).alias("Avg_Severity_Per_Accident")
        )
    )

    # Define PySpark Window Specifications
    window_spec_row = Window.partitionBy("Urban_or_Rural_Area").orderBy(F.desc("Severity_Score"), F.desc("Total_Accidents"))
    window_spec_rank = Window.partitionBy("Urban_or_Rural_Area").orderBy(F.desc("Severity_Score"))
    window_spec_dense = Window.partitionBy("Urban_or_Rural_Area").orderBy(F.desc("Severity_Score"))

    # Apply Window ranking functions (row_number, rank, dense_rank)
    ranked_locations = (
        location_category_agg
        .withColumn("Row_Num", F.row_number().over(window_spec_row))
        .withColumn("Rank", F.rank().over(window_spec_rank))
        .withColumn("Dense_Rank", F.dense_rank().over(window_spec_dense))
    )

    # Demonstrate Multi-Condition Filtering on Window Output (& operator)
    top3_per_category = (
        ranked_locations
        .filter((F.col("Row_Num") <= 3) & (F.col("Urban_or_Rural_Area") != "Unknown"))
        .orderBy("Urban_or_Rural_Area", "Row_Num")
    )

    print("--- TOP 3 HIGHEST-RISK LOCATIONS WITHIN EACH GEOGRAPHICAL CATEGORY ---")
    top3_per_category.show(truncate=False)

    # Demonstrate select() with col().alias() on Window Output
    print("\n--- WINDOW RANKING COMPARISON (Row_Num vs Rank vs Dense_Rank) ---")
    top3_per_category.select(
        "Urban_or_Rural_Area",
        "Local_Authority_District",
        "Severity_Score",
        "Row_Num",
        "Rank",
        "Dense_Rank",
        (F.col("Severity_Score") / F.col("Total_Accidents")).alias("Severity_Per_Crash")
    ).show(truncate=False)

    # Save output to CSV
    output_dir = os.path.join(REPO_ROOT, "output", "task6_window_ranking")
    os.makedirs(output_dir, exist_ok=True)
    top3_per_category.write.mode("overwrite").csv(
        os.path.join(output_dir, "top3_locations_per_category.csv"), header=True
    )

    print(f"[Task 6] Window ranking complete. Written to: {output_dir}")
    print("==========================================================\n")

    if own_spark:
        spark.stop()

    return top3_per_category


if __name__ == "__main__":
    run()
