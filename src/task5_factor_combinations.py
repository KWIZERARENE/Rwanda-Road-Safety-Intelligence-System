"""
Task 5: Dangerous-Factor Combination Analysis (RRSIS Project)
--------------------------------------------------------------
- Investigates multi-dimensional risk factor tuples across Road Type, Speed Limit,
  Weather Conditions, Road Surface Conditions, Light Conditions, Time Period, and Vehicle Type.
- Ranks the Top 10 dangerous factor combinations based on Total Severity Score and Fatality Ratio.

Run standalone:
    python src/task5_factor_combinations.py
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from src.spark_session import get_spark_session
    from src.task4_severity_index import run as run_task4
except ImportError:
    from spark_session import get_spark_session
    from task4_severity_index import run as run_task4


def run(spark=None, df_sev=None):
    own_spark = spark is None
    if own_spark:
        spark = get_spark_session("RRSIS_Task5_FactorCombinations")

    if df_sev is None:
        df_sev = run_task4(spark)

    print("\n==========================================================")
    print(" TASK 5: DANGEROUS-FACTOR COMBINATION ANALYSIS")
    print("==========================================================")

    # Multi-factor grouping attributes
    factor_cols = [
        "Road_Type", "Speed_limit", "Weather_Conditions",
        "Road_Surface_Conditions", "Light_Conditions", "Time_Period"
    ]
    
    # Filter available columns
    available_factors = [c for c in factor_cols if c in df_sev.columns]
    print(f"Analyzing combinations using factors: {available_factors}\n")

    top10_combinations = (
        df_sev.groupBy(*available_factors)
        .agg(
            F.count("*").alias("Accident_Count"),
            F.sum("Severity_Weight").alias("Total_Severity_Score"),
            F.sum(F.when(F.col("Accident_Severity") == "Fatal", 1).otherwise(0)).alias("Fatal_Accidents"),
            F.sum(F.when(F.col("Accident_Severity") == "Serious", 1).otherwise(0)).alias("Serious_Accidents"),
            F.round(F.avg("Severity_Weight"), 2).alias("Avg_Severity_Per_Accident")
        )
        .withColumn(
            "Fatality_Rate_Pct",
            F.round((F.col("Fatal_Accidents") / F.col("Accident_Count")) * 100, 2)
        )
        .orderBy(F.desc("Total_Severity_Score"), F.desc("Fatal_Accidents"))
        .limit(10)
    )

    print("--- TOP 10 DANGEROUS FACTOR COMBINATIONS (RANKED BY SEVERITY SCORE) ---")
    top10_combinations.show(10, truncate=False)

    output_dir = os.path.join("output", "task5_factor_combinations")
    os.makedirs(output_dir, exist_ok=True)
    top10_combinations.write.mode("overwrite").csv(
        os.path.join(output_dir, "top10_factor_combinations.csv"), header=True
    )

    print(f"[Task 5] Dangerous factor combination analysis complete. Written to: {output_dir}")
    print("==========================================================\n")

    if own_spark:
        spark.stop()

    return top10_combinations


if __name__ == "__main__":
    run()
