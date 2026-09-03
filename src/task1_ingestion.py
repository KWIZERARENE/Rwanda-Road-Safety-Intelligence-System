"""
Task 1: HDFS Data Ingestion Module (RRSIS Project)
--------------------------------------------------
Reads dataset strictly from the HDFS path configured in src/config.py:
    HDFS_DATASET_PATH = "hdfs://localhost:9000/user/hadoop/my_dataset/Road Accident Data.csv"

To change the dataset location, update src/config.py.

Run standalone:
    python src/task1_ingestion.py
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from src.config import HDFS_DATASET_PATH
    from src.spark_session import get_spark_session
except ImportError:
    from config import HDFS_DATASET_PATH
    from spark_session import get_spark_session


def load_dataset(spark):
    """
    Ingests the accident dataset directly from HDFS using the HDFS path
    defined in src/config.py.
    
    Parameters:
        spark (SparkSession): Active PySpark session.
        
    Returns:
        DataFrame: Raw ingested PySpark DataFrame from HDFS.
    """
    print(f"[HDFS Ingestion Module] Loading dataset from HDFS:")
    print(f"  Target URI: {HDFS_DATASET_PATH}")
    
    df = spark.read.csv(HDFS_DATASET_PATH, header=True, inferSchema=True)
    return df


def run(spark=None):
    own_spark = spark is None
    if own_spark:
        spark = get_spark_session("RRSIS_Task1_Ingestion")

    print("\n==========================================================")
    print(" TASK 1: HDFS + SPARK DATA INGESTION")
    print("==========================================================")

    df_raw = load_dataset(spark)

    total_records = df_raw.count()
    total_columns = len(df_raw.columns)
    column_names = df_raw.columns

    print(f"\n--- DATASET INGESTION PROPERTIES ---")
    print(f" Data Source:      HDFS ({HDFS_DATASET_PATH})")
    print(f" Total Records:    {total_records:,}")
    print(f" Total Columns:    {total_columns}")
    print(f" Column Schema List:")
    for idx, col in enumerate(column_names, start=1):
        print(f"   {idx:02d}. {col}")

    print("\n--- INGESTED DATAFRAME SCHEMA ---")
    df_raw.printSchema()

    print("\n--- SAMPLE INGESTED ACCIDENT RECORDS (Top 5) ---")
    df_raw.show(5, truncate=False)

    # Save ingestion summary report
    output_dir = os.path.join("output", "task1_ingestion")
    os.makedirs(output_dir, exist_ok=True)
    summary_path = os.path.join(output_dir, "ingestion_summary.txt")
    
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("==========================================================\n")
        f.write(" TASK 1: HDFS DATA INGESTION REPORT\n")
        f.write("==========================================================\n")
        f.write(f"HDFS Target URI: {HDFS_DATASET_PATH}\n")
        f.write(f"Total Record Count: {total_records}\n")
        f.write(f"Total Column Count: {total_columns}\n\n")
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
