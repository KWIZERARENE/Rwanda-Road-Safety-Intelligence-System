"""
Task 1: HDFS + Spark Data Ingestion Module (RRSIS Project)
----------------------------------------------------------
Reads dataset directly from HDFS using PySpark DataFrame API:
    HDFS_DATASET_PATH = "hdfs://localhost:9000/user/hadoop/my_dataset/Road Accident Data.csv"

Demonstrates:
  - Ingestion options: option("header", "true").option("inferSchema", "true")
  - Schema printing: printSchema()
  - Statistical summary: describe().show()
  - Partition count inspection: df.rdd.getNumPartitions()
  - Partition ID tracking: spark_partition_id()
  - Partition row count aggregation & distribution checks
  - Representative dataset sampling: sample(False, 0.10)
  - Display with custom formatting: show(n, vertical=False)

Run standalone:
    python src/task1_ingestion.py
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

try:
    from src.config import HDFS_DATASET_PATH, LOCAL_DATASET_PATH
    from src.spark_session import get_spark_session
except ImportError:
    from config import HDFS_DATASET_PATH, LOCAL_DATASET_PATH
    from spark_session import get_spark_session


def load_dataset(spark):
    """
    Ingests the accident dataset directly from HDFS using the HDFS path
    defined in src/config.py. Provides local fallback if HDFS is unavailable.
    """
    print(f"[HDFS Ingestion Module] Loading dataset from HDFS:")
    print(f"  Target URI: {HDFS_DATASET_PATH}")
    
    try:
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(HDFS_DATASET_PATH)
        return df
    except Exception as e:
        if os.path.exists(LOCAL_DATASET_PATH):
            print(f"[HDFS Notice] HDFS path not reachable ({e}).")
            print(f"               Using local fallback dataset: {LOCAL_DATASET_PATH}")
            return spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(LOCAL_DATASET_PATH)
        else:
            raise e


def run(spark=None):
    own_spark = spark is None
    if own_spark:
        spark = get_spark_session("RRSIS_Task1_Ingestion")

    print("\n==========================================================")
    print(" TASK 1: HDFS + SPARK DATA INGESTION & PARTITION DIAGNOSTICS")
    print("==========================================================")

    # 1. Ingest Data from HDFS
    df_raw = load_dataset(spark)

    total_records = df_raw.count()
    total_columns = len(df_raw.columns)
    column_names = df_raw.columns

    print(f"\n--- 1. DATASET INGESTION PROPERTIES ---")
    print(f" Data Source Target: HDFS ({HDFS_DATASET_PATH})")
    print(f" Total Records:      {total_records:,}")
    print(f" Total Columns:      {total_columns}")
    print(f" Column Schema List:")
    for idx, col in enumerate(column_names, start=1):
        print(f"   {idx:02d}. {col}")

    # 2. Schema Printing
    print("\n--- 2. INGESTED DATAFRAME SCHEMA (printSchema) ---")
    df_raw.printSchema()

    # 3. Descriptive Statistics Summary
    print("\n--- 3. DATASET STATISTICAL SUMMARY (describe().show()) ---")
    df_raw.describe().show(vertical=False)

    # 4. HDFS & Spark Partition Analysis (rdd.getNumPartitions, spark_partition_id)
    num_partitions = df_raw.rdd.getNumPartitions()
    print(f"\n--- 4. SPARK PARTITION DIAGNOSTICS ---")
    print(f" Number of Spark RDD Partitions: {num_partitions}")

    # Add partition_id column using PySpark function spark_partition_id()
    df_partitioned = df_raw.withColumn("partition_id", F.spark_partition_id())

    print("\nPartition Row Distribution (groupBy partition_id):")
    df_partitioned.groupBy("partition_id") \
        .count() \
        .orderBy("partition_id") \
        .show()

    # Cross-partition breakdown if Accident_Severity or key column is present
    if "Accident_Severity" in df_raw.columns:
        print("Cross-Partition Category Breakdown (partition_id + Accident_Severity):")
        df_partitioned.groupBy("partition_id", "Accident_Severity") \
            .count() \
            .orderBy("partition_id", "Accident_Severity") \
            .show(100)

    # 5. Representative Data Sampling (sample)
    print("\n--- 5. REPRESENTATIVE SAMPLE (sample(False, 0.10)) ---")
    sample_df = df_partitioned.sample(withReplacement=False, fraction=0.10)
    sample_df.show(5, vertical=False)

    # 6. Top Sample Records
    print("\n--- 6. SAMPLE INGESTED ACCIDENT RECORDS (Top 5) ---")
    df_raw.show(5, truncate=False, vertical=False)

    # Save ingestion summary report
    output_dir = os.path.join(REPO_ROOT, "output", "task1_ingestion")
    os.makedirs(output_dir, exist_ok=True)
    summary_path = os.path.join(output_dir, "ingestion_summary.txt")
    
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("==========================================================\n")
        f.write(" TASK 1: HDFS DATA INGESTION & PARTITION REPORT\n")
        f.write("==========================================================\n")
        f.write(f"HDFS Target URI: {HDFS_DATASET_PATH}\n")
        f.write(f"Total Record Count: {total_records}\n")
        f.write(f"Total Column Count: {total_columns}\n")
        f.write(f"Spark RDD Partition Count: {num_partitions}\n\n")
        f.write("Columns:\n")
        for col in column_names:
            f.write(f" - {col}\n")

    print(f"[Task 1] Ingestion summary report written to: {summary_path}")
    print("==========================================================\n")

    if own_spark:
        spark.stop()

    return df_raw


if __name__ == "__main__":
    run()
