"""
Task 4: Accident Severity Index (RRSIS Project)
-----------------------------------------------
- Assigns explicit severity weights: Slight (1), Serious (3), Fatal (5).
- Computes Severity Score = sum(Accident_Count * Severity_Weight) across dimensions:
  Location (District), Road Type, Vehicle Type, and Time Period.
- Demonstrates:
  - Conditional withColumn() severity weight assignment
  - Arithmetic operations inside withColumn()
  - Multi-condition filtering with & operator
  - Statistical diagnostics via describe()
  - Column calculation & renaming using select(..., col().alias())
  - Grouped aggregations with groupBy(), agg(), sum(), count(), avg(), round(), orderBy()
- Identifies locations with the highest severity burden.
- Explains why high accident volume does not equal maximum safety risk.

Run standalone:
    python src/task4_severity_index.py
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
    from src.task3_temporal_analysis import run as run_task3
except ImportError:
    from spark_session import get_spark_session
    from task3_temporal_analysis import run as run_task3


def add_severity_weights(df):
    """
    Applies explicit weights: Slight -> 1, Serious -> 3, Fatal -> 5.
    Also demonstrates arithmetic withColumn operation.
    """
    df = df.withColumn(
        "Severity_Weight",
        F.when(F.col("Accident_Severity") == "Slight", 1)
         .when(F.col("Accident_Severity") == "Serious", 3)
         .when(F.col("Accident_Severity") == "Fatal", 5)
         .otherwise(1)
    )
    # Demonstrate arithmetic withColumn operation
    df = df.withColumn("Severity_Weight_Double", F.col("Severity_Weight") * 2)
    return df


def run(spark=None, df_temp=None):
    own_spark = spark is None
    if own_spark:
        spark = get_spark_session("RRSIS_Task4_Severity")

    if df_temp is None:
        df_temp = run_task3(spark)

    print("\n==========================================================")
    print(" TASK 4: ACCIDENT SEVERITY INDEX ANALYSIS")
    print("==========================================================")

    df_sev = add_severity_weights(df_temp)
    total_accidents = df_sev.count()

    # Demonstrate Statistical Summary on Severity Metrics
    print("--- SEVERITY WEIGHT STATISTICAL SUMMARY (describe) ---")
    df_sev.select("Severity_Weight", "Severity_Weight_Double").describe().show(vertical=False)

    # Demonstrate Multi-Condition Filtering (High Severity & Specific Road Type)
    if "Road_Type" in df_sev.columns:
        print("--- HIGH SEVERITY FILTERING (Severity_Weight >= 3 & Road_Type != 'Unknown') ---")
        severe_crashes_df = df_sev.filter(
            (F.col("Severity_Weight") >= 3) & 
            (F.col("Road_Type") != "Unknown")
        )
        print(f" Total Severe Crashes (Serious/Fatal): {severe_crashes_df.count():,}")
        severe_crashes_df.select("Accident_Severity", "Severity_Weight", "Road_Type").show(5)

    # 1. Severity Score by Location (District / Local Authority)
    print("\n--- 1. SEVERITY SCORE BY LOCATION (DISTRICT) ---")
    location_severity = (
        df_sev.groupBy("Local_Authority_District")
        .agg(
            F.count("*").alias("Total_Accidents"),
            F.sum("Severity_Weight").alias("Severity_Score"),
            F.sum(F.when(F.col("Accident_Severity") == "Fatal", 1).otherwise(0)).alias("Fatal_Count"),
            F.sum(F.when(F.col("Accident_Severity") == "Serious", 1).otherwise(0)).alias("Serious_Count"),
            F.sum(F.when(F.col("Accident_Severity") == "Slight", 1).otherwise(0)).alias("Slight_Count"),
            F.round(F.avg("Severity_Weight"), 2).alias("Avg_Severity_Per_Accident")
        )
        .orderBy(F.desc("Severity_Score"))
    )
    location_severity.show(10, truncate=False)

    # Demonstrate Inline Calculation with select and alias()
    print("Location Severity Index Ratio Calculation (select with alias):")
    location_severity.select(
        "Local_Authority_District",
        "Total_Accidents",
        "Severity_Score",
        (F.col("Severity_Score") / F.col("Total_Accidents")).alias("Severity_To_Accident_Ratio")
    ).show(5, truncate=False)

    # 2. Severity Score by Road Type
    print("\n--- 2. SEVERITY SCORE BY ROAD TYPE ---")
    road_severity = (
        df_sev.groupBy("Road_Type")
        .agg(
            F.count("*").alias("Total_Accidents"),
            F.sum("Severity_Weight").alias("Severity_Score"),
            F.round(F.avg("Severity_Weight"), 2).alias("Avg_Severity_Per_Accident")
        )
        .orderBy(F.desc("Severity_Score"))
    )
    road_severity.show(truncate=False)

    # 3. Severity Score by Vehicle Type
    print("\n--- 3. SEVERITY SCORE BY VEHICLE TYPE ---")
    vehicle_severity = (
        df_sev.groupBy("Vehicle_Type")
        .agg(
            F.count("*").alias("Total_Accidents"),
            F.sum("Severity_Weight").alias("Severity_Score"),
            F.round(F.avg("Severity_Weight"), 2).alias("Avg_Severity_Per_Accident")
        )
        .orderBy(F.desc("Severity_Score"))
    )
    vehicle_severity.show(truncate=False)

    # 4. Severity Score by Time Period
    print("\n--- 4. SEVERITY SCORE BY TIME PERIOD ---")
    time_severity = (
        df_sev.groupBy("Time_Period")
        .agg(
            F.count("*").alias("Total_Accidents"),
            F.sum("Severity_Weight").alias("Severity_Score"),
            F.round(F.avg("Severity_Weight"), 2).alias("Avg_Severity_Per_Accident")
        )
        .orderBy(F.desc("Severity_Score"))
    )
    time_severity.show(truncate=False)

    # Analytical Explanation for Task 4
    explanation = """
================================================================================
 KEY ANALYTICAL INSIGHT: ACCIDENT FREQUENCY VS. SEVERITY RISK BURDEN
================================================================================
 Why the location with the most accidents is NOT necessarily the location with 
 the greatest road-safety risk:

 1. Frequency Masking:
    A high-density urban junction may experience 100 minor fender-benders (Slight,
    weight=1), resulting in a Severity Score of 100. Conversely, a rural arterial
    highway may experience only 30 accidents, but 10 are Fatal (weight=5) and 15 
    are Serious (weight=3), yielding a Severity Score of (10*5)+(15*3)+(5*1) = 100.

 2. Human & Economic Impact:
    Fatal and serious accidents impose severe human trauma, medical costs, and 
    economic loss compared to property-damage-only or slight injury incidents.

 3. Prioritization Focus:
    Resource allocation based purely on accident counts misdirects enforcement to
    minor low-speed urban spots, while high-speed fatal corridors remain unaddressed.
================================================================================
"""
    print(explanation)

    output_dir = os.path.join(REPO_ROOT, "output", "task4_severity")
    os.makedirs(output_dir, exist_ok=True)
    location_severity.write.mode("overwrite").csv(os.path.join(output_dir, "location_severity_index.csv"), header=True)
    
    with open(os.path.join(output_dir, "severity_explanation.txt"), "w", encoding="utf-8") as f:
        f.write(explanation)

    print(f"[Task 4] Severity analysis complete. Output written to: {output_dir}")
    print("==========================================================\n")

    if own_spark:
        spark.stop()

    return df_sev


if __name__ == "__main__":
    run()
