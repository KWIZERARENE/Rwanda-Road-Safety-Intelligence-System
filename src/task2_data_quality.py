"""
Task 2: Data Quality Engineering (RRSIS Project)
------------------------------------------------
- Diagnoses missing values, duplicate records, data type anomalies, and invalid entries.
- Documents 6 distinct data quality issues with analytical justification for treatments.
- Demonstrates:
  - Statistical diagnostics via describe().show()
  - Multi-condition boolean filtering using filter((cond1) & (cond2))
  - Column calculation & renaming using select(..., col().alias())
  - Arithmetic transformations with withColumn()
  - Step-by-step intermediate DataFrame creation
  - Partition integrity check with spark_partition_id()
  - PySpark DataFrame sanitization pipeline (casing standardization, key deduplication,
    imputation with 'Unknown', coordinate nullification, dropping unusable rows).

Run standalone:
    python src/task2_data_quality.py
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
    from src.spark_session import get_spark_session
    from src.task1_ingestion import run as run_task1
except ImportError:
    from spark_session import get_spark_session
    from task1_ingestion import run as run_task1


def standardize_column_names(df):
    """Clean header strings by replacing whitespace and special characters with underscores."""
    for col_name in df.columns:
        clean_name = col_name.strip().replace(" ", "_").replace("/", "_").replace("-", "_").replace("(", "").replace(")", "")
        df = df.withColumnRenamed(col_name, clean_name)
    return df


def audit_null_and_blank_counts(df):
    """Computes exact counts of NULL, NaN, or whitespace-only blank values per column."""
    exprs = [
        F.sum(
            F.when(
                F.col(c).isNull() |
                F.isnan(c) |
                (F.trim(F.col(c).cast("string")) == "") |
                (F.col(c) == "None") |
                (F.col(c) == "Null"),
                1
            ).otherwise(0)
        ).alias(c)
        for c in df.columns
    ]
    return df.select(exprs)


def run(spark=None, df_raw=None):
    own_spark = spark is None
    if own_spark:
        spark = get_spark_session("RRSIS_Task2_DataQuality")

    if df_raw is None:
        df_raw = run_task1(spark)

    print("\n==========================================================")
    print(" TASK 2: DATA QUALITY ENGINEERING & SANITIZATION")
    print("==========================================================")

    # Step 1: Standardize Header Column Names
    df_step1 = standardize_column_names(df_raw)
    raw_count = df_step1.count()
    print(f"Total raw accident records loaded: {raw_count:,}\n")

    # Demonstrate Statistical Summary on Raw Data
    print("--- 1A. RAW DATAFRAME STATISTICAL SUMMARY (describe) ---")
    df_step1.describe().show(vertical=False)

    # Demonstrate Column Slicing & Inline Calculation with alias()
    if "Speed_limit" in df_step1.columns:
        print("--- 1B. COLUMN SELECTION & INLINE ALIAS CALCULATION ---")
        df_step1.select(
            "Accident_Severity",
            "Speed_limit",
            (F.col("Speed_limit") * 1.60934).alias("Speed_limit_kmh")
        ).show(5, truncate=False)

    # Demonstrate Multi-Condition Filtering with Bitwise &
    if "Speed_limit" in df_step1.columns and "Accident_Severity" in df_step1.columns:
        print("--- 1C. MULTI-CONDITION BOOLEAN FILTERING (filter with &) ---")
        high_risk_filtered_df = df_step1.filter(
            (F.col("Speed_limit") > 30) & 
            (F.col("Accident_Severity").isin("Fatal", "Fetal", "Serious"))
        )
        print(f" Records matching (Speed_limit > 30 & High Severity): {high_risk_filtered_df.count():,}")
        high_risk_filtered_df.select("Accident_Severity", "Speed_limit").show(5)

    # 1. Audit Missing & Blank Values
    print("\n--- 2. MISSING / BLANK VALUES AUDIT PER COLUMN ---")
    null_report_df = audit_null_and_blank_counts(df_step1)
    null_report_df.show(vertical=True, truncate=False)

    # 2. Duplicate Record Inspection
    print("--- 3. DUPLICATE RECORD DIAGNOSTICS ---")
    exact_distinct_count = df_step1.dropDuplicates().count()
    exact_duplicates = raw_count - exact_distinct_count
    print(f" Exact Duplicate Rows Count: {exact_duplicates}")

    if "Accident_Index" in df_step1.columns:
        duplicate_indices = (
            df_step1.groupBy("Accident_Index")
            .count()
            .filter(F.col("count") > 1)
        )
        print(f" Duplicate 'Accident_Index' Primary Keys Count: {duplicate_indices.count()}")

    # 3. Suspicious and Inappropriate Values Check
    print("\n--- 4. SUSPICIOUS AND INAPPROPRIATE VALUES AUDIT ---")
    
    if "Accident_Severity" in df_step1.columns:
        print("\nDistinct Accident_Severity values (checking for spelling errors):")
        df_step1.groupBy("Accident_Severity").count().orderBy(F.desc("count")).show(truncate=False)

    if "Speed_limit" in df_step1.columns:
        print("\nDistinct Speed_limit values (checking for 0 or negative values):")
        df_step1.groupBy("Speed_limit").count().orderBy("Speed_limit").show(15, truncate=False)

    if "Latitude" in df_step1.columns and "Longitude" in df_step1.columns:
        zero_coords = df_step1.filter(
            F.col("Latitude").isNull() | F.col("Longitude").isNull() |
            (F.col("Latitude") == 0) | (F.col("Longitude") == 0)
        ).count()
        print(f"\nRecords with Missing or Placeholder Coordinates (0.0, 0.0): {zero_coords}")

    # 4. Documented Data-Quality Issues with Analytical Justification
    dq_issues_text = """
================================================================================
 DOCUMENTED DATA QUALITY ISSUES & JUSTIFIED SANITIZATION TREATMENTS
================================================================================
 1. Missing / Blank Categorical Fields:
    - Issue: Fields such as 'Carriageway_Hazards', 'Road_Surface_Conditions', and
      'Weather_Conditions' contain blank strings or 'None' labels.
    - Justification: Simply dropping rows missing minor descriptive attributes 
      would discard critical casualty/fatality figures. Imputing with 'Unknown'
      preserves record counts while preventing split categories.

 2. Typographical Misspellings in Severity Labels:
    - Issue: 'Accident_Severity' contains typos (e.g., 'Fetal' instead of 'Fatal').
    - Justification: Unmapped typos distort Severity Weight Index computations.
      Standardizing 'Fetal' -> 'Fatal' ensures exact weighted severity scoring.

 3. Duplicate Primary Key Records ('Accident_Index'):
    - Issue: Duplicate rows with identical 'Accident_Index' exist in raw data.
    - Justification: Exact duplicate rows skew frequency statistics and artificially
      inflate risk scores. Applying dropDuplicates() eliminates double counting.

 4. Out-of-Bounds & Zero Speed Limits:
    - Issue: 'Speed_limit' contains zero (0) and non-positive entries.
    - Justification: 0 mph is non-physical for traffic accident records. Recasting
      non-positive speed limits to NULL prevents skewing average speed calculations.

 5. Coordinate Placeholders (0.0, 0.0):
    - Issue: Geographic coordinates ('Latitude', 'Longitude') feature (0.0, 0.0).
    - Justification: Setting zero coordinates to NULL prevents false spatial aggregation
      at the equator while preserving non-spatial attribute analysis.

 6. Casing & Trailing Whitespace Inconsistencies:
    - Issue: Inconsistent capitalization and whitespace (e.g., 'Fine ' vs 'fine').
    - Justification: Applying F.trim() and F.initcap() normalizes categories so that
      groupBy aggregations produce accurate, unified metrics.
================================================================================
"""
    print(dq_issues_text)

    # 5. Execute Justified Sanitization Pipeline Step-by-Step
    print("--- 5. EXECUTING PYSPARK DATA SANITIZATION PIPELINE ---")

    df_step2 = df_step1

    # (a) Standardize casing & strip whitespace across categorical columns
    categorical_cols = [
        c for c in [
            "Accident_Severity", "Road_Type", "Weather_Conditions",
            "Road_Surface_Conditions", "Light_Conditions", "Junction_Control",
            "Junction_Detail", "Vehicle_Type", "Urban_or_Rural_Area",
            "Carriageway_Hazards", "Day_of_Week", "Local_Authority_District"
        ] if c in df_step2.columns
    ]
    for c in categorical_cols:
        df_step2 = df_step2.withColumn(c, F.initcap(F.trim(F.col(c).cast("string"))))

    # (b) Correct critical typographical errors
    if "Accident_Severity" in df_step2.columns:
        df_step2 = df_step2.withColumn(
            "Accident_Severity",
            F.when(F.col("Accident_Severity") == "Fetal", "Fatal")
             .otherwise(F.col("Accident_Severity"))
        )

    # (c) Deduplicate exact identical records
    df_step2 = df_step2.dropDuplicates()

    # (d) Impute missing categorical attributes with 'Unknown'
    for c in categorical_cols:
        df_step2 = df_step2.withColumn(
            c,
            F.when(
                F.col(c).isNull() | (F.trim(F.col(c)) == "") | (F.col(c) == "None"),
                "Unknown"
            ).otherwise(F.col(c))
        )

    # (e) Nullify placeholder coordinates (0.0, 0.0)
    if "Latitude" in df_step2.columns and "Longitude" in df_step2.columns:
        df_step2 = df_step2.withColumn(
            "Latitude", F.when(F.col("Latitude") == 0, None).otherwise(F.col("Latitude"))
        ).withColumn(
            "Longitude", F.when(F.col("Longitude") == 0, None).otherwise(F.col("Longitude"))
        )

    # (f) Clean out-of-bounds speed limits and create Speed_limit_kmh via arithmetic withColumn
    if "Speed_limit" in df_step2.columns:
        df_step2 = df_step2.withColumn(
            "Speed_limit",
            F.when(F.col("Speed_limit") <= 0, None).otherwise(F.col("Speed_limit"))
        ).withColumn(
            "Speed_limit_kmh",
            F.when(F.col("Speed_limit").isNotNull(), F.round(F.col("Speed_limit") * 1.60934, 1))
             .otherwise(None)
        )

    # (g) Filter records missing essential analytical keys
    essential_cols = [c for c in ["Accident_Severity", "Accident_Date"] if c in df_step2.columns]
    if essential_cols:
        before_drop = df_step2.count()
        df_step2 = df_step2.dropna(subset=essential_cols)
        after_drop = df_step2.count()
        print(f" Filtered {before_drop - after_drop} unusable records missing critical fields {essential_cols}.")

    df_clean = df_step2
    sanitized_count = df_clean.count()

    # Verify Partition Distribution After Cleaning
    df_clean_partitioned = df_clean.withColumn("partition_id", F.spark_partition_id())
    print("\nCleaned Dataset Partition Distribution (spark_partition_id):")
    df_clean_partitioned.groupBy("partition_id").count().orderBy("partition_id").show()

    print(f"\n Sanitization Pipeline Complete.")
    print(f" Raw Count:       {raw_count:,}")
    print(f" Cleaned Count:   {sanitized_count:,}")
    print(f" Net Retention:   {(sanitized_count / raw_count) * 100:.2f}%\n")

    # Save output summary
    output_dir = os.path.join(REPO_ROOT, "output", "task2_data_quality")
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "data_quality_report.txt"), "w", encoding="utf-8") as f:
        f.write(dq_issues_text)
        f.write(f"\nRaw Records: {raw_count}\nCleaned Records: {sanitized_count}\n")

    print(f"[Task 2] Data quality report saved to: {os.path.join(output_dir, 'data_quality_report.txt')}")
    print("==========================================================\n")

    if own_spark:
        spark.stop()

    return df_clean


if __name__ == "__main__":
    run()
