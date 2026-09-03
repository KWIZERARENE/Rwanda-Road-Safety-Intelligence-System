"""
Task 3: Temporal Accident Intelligence (RRSIS Project)
------------------------------------------------------
- Analyzes accident distribution across temporal dimensions: Hour of Day, Day of Week, Month, and Weekday vs Weekend.
- Derives custom time window categories: Late Night, Morning, Afternoon, Evening, Night.
- Ranks the 5 highest-risk temporal periods with exact numerical evidence.

Run standalone:
    python src/task3_temporal_analysis.py
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from src.spark_session import get_spark_session
    from src.task2_data_quality import run as run_task2
except ImportError:
    from spark_session import get_spark_session
    from task2_data_quality import run as run_task2


def add_temporal_features(df):
    """
    Parses date/time strings and derives analytical temporal attributes:
    Hour_of_Day, Day_of_Week, Month, Is_Weekend, and Time_Period.
    """
    # Parse Hour of Day from Time string (HH:mm format or timestamp)
    df = df.withColumn(
        "Hour_of_Day",
        F.when(F.col("Time").contains(":"), F.split(F.col("Time"), ":").getItem(0).cast("integer"))
         .otherwise(F.hour(F.col("Accident_Date")))
    )
    
    # Fill any null hour with 12 as default median
    df = df.withColumn("Hour_of_Day", F.coalesce(F.col("Hour_of_Day"), F.lit(12)))

    # Parse Month and Day of Week if not present
    if "Month" not in df.columns:
        df = df.withColumn("Month", F.month(F.col("Accident_Date")))
        
    # Weekday vs Weekend flag
    df = df.withColumn(
        "Is_Weekend",
        F.when(F.col("Day_of_Week").isin("Saturday", "Sunday"), "Weekend").otherwise("Weekday")
    )

    # Derive custom time period categories
    # Late Night: 00:00 - 04:59
    # Morning:    05:00 - 11:59
    # Afternoon:  12:00 - 16:59
    # Evening:    17:00 - 20:59
    # Night:      21:00 - 23:59
    df = df.withColumn(
        "Time_Period",
        F.when((F.col("Hour_of_Day") >= 0) & (F.col("Hour_of_Day") <= 4), "Late Night")
         .when((F.col("Hour_of_Day") >= 5) & (F.col("Hour_of_Day") <= 11), "Morning")
         .when((F.col("Hour_of_Day") >= 12) & (F.col("Hour_of_Day") <= 16), "Afternoon")
         .when((F.col("Hour_of_Day") >= 17) & (F.col("Hour_of_Day") <= 20), "Evening")
         .when((F.col("Hour_of_Day") >= 21) & (F.col("Hour_of_Day") <= 23), "Night")
         .otherwise("Unknown")
    )
    return df


def run(spark=None, df_clean=None):
    own_spark = spark is None
    if own_spark:
        spark = get_spark_session("RRSIS_Task3_Temporal")

    if df_clean is None:
        df_clean = run_task2(spark)

    print("\n==========================================================")
    print(" TASK 3: TEMPORAL ACCIDENT INTELLIGENCE ANALYSIS")
    print("==========================================================")

    df_temp = add_temporal_features(df_clean)
    total_accidents = df_temp.count()

    # 1. Accidents by Custom Time Period Category
    print("--- 1. ACCIDENT CONCENTRATION BY TIME PERIOD CATEGORY ---")
    period_df = (
        df_temp.groupBy("Time_Period")
        .agg(
            F.count("*").alias("Accident_Count"),
            F.sum(F.when(F.col("Accident_Severity") == "Fatal", 1).otherwise(0)).alias("Fatal_Accidents"),
            F.sum(F.when(F.col("Accident_Severity") == "Serious", 1).otherwise(0)).alias("Serious_Accidents")
        )
        .withColumn("Percentage", F.round((F.col("Accident_Count") / total_accidents) * 100, 2))
        .orderBy(F.desc("Accident_Count"))
    )
    period_df.show(truncate=False)

    # 2. Weekday vs Weekend Analysis
    print("\n--- 2. ACCIDENTS BY WEEKDAY VS WEEKEND ---")
    weekend_df = (
        df_temp.groupBy("Is_Weekend")
        .agg(
            F.count("*").alias("Accident_Count"),
            F.sum(F.when(F.col("Accident_Severity") == "Fatal", 1).otherwise(0)).alias("Fatal_Accidents"),
            F.sum(F.when(F.col("Accident_Severity") == "Serious", 1).otherwise(0)).alias("Serious_Accidents")
        )
        .withColumn("Percentage", F.round((F.col("Accident_Count") / total_accidents) * 100, 2))
        .orderBy(F.desc("Accident_Count"))
    )
    weekend_df.show(truncate=False)

    # 3. Accidents by Day of Week
    print("\n--- 3. ACCIDENTS BY DAY OF WEEK ---")
    day_df = (
        df_temp.groupBy("Day_of_Week")
        .agg(
            F.count("*").alias("Accident_Count"),
            F.sum(F.when(F.col("Accident_Severity") == "Fatal", 1).otherwise(0)).alias("Fatal_Accidents")
        )
        .withColumn("Percentage", F.round((F.col("Accident_Count") / total_accidents) * 100, 2))
        .orderBy(F.desc("Accident_Count"))
    )
    day_df.show(truncate=False)

    # 4. Top 5 Highest-Risk Time Periods (Combination of Day of Week + Time Period)
    print("\n--- 4. TOP 5 HIGHEST-RISK TEMPORAL WINDOWS (Day + Time Window) ---")
    top5_temporal = (
        df_temp.groupBy("Day_of_Week", "Time_Period")
        .agg(
            F.count("*").alias("Accident_Count"),
            F.sum(F.when(F.col("Accident_Severity") == "Fatal", 1).otherwise(0)).alias("Fatal_Count"),
            F.sum(F.when(F.col("Accident_Severity") == "Serious", 1).otherwise(0)).alias("Serious_Count")
        )
        .withColumn(
            "Fatality_Rate_Pct", 
            F.round((F.col("Fatal_Count") / F.col("Accident_Count")) * 100, 2)
        )
        .orderBy(F.desc("Accident_Count"), F.desc("Fatal_Count"))
        .limit(5)
    )
    top5_temporal.show(truncate=False)

    # Save output to CSV & report
    output_dir = os.path.join("output", "task3_temporal")
    os.makedirs(output_dir, exist_ok=True)
    top5_temporal.write.mode("overwrite").csv(os.path.join(output_dir, "top5_temporal_windows.csv"), header=True)

    print(f"[Task 3] Temporal analysis complete. Output written to: {output_dir}")
    print("==========================================================\n")

    if own_spark:
        spark.stop()

    return df_temp


if __name__ == "__main__":
    run()
