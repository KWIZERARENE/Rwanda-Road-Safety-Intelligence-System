"""
Task 7: Composite Road Safety Risk Score (RRSIS Project)
--------------------------------------------------------
- Builds a normalized, multi-dimensional Composite Road Safety Risk Score (0-100 scale).
- Combines:
    1. Normalized Accident Frequency (Weight = 0.35)
    2. Normalized Weighted Severity Score (Weight = 0.40)
    3. Normalized High-Risk Condition Proportion (Night/Rain/High Speed) (Weight = 0.25)
- Ranks locations for targeted police and engineering intervention.

Run standalone:
    python src/task7_risk_score.py
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
        spark = get_spark_session("RRSIS_Task7_RiskScore")

    if df_sev is None:
        df_sev = run_task4(spark)

    print("\n==========================================================")
    print(" TASK 7: COMPOSITE ROAD SAFETY RISK SCORE MODEL")
    print("==========================================================")

    # 1. Define high-risk adverse condition flag per accident
    # Adverse conditions: Night/Late Night, Wet/Snow/Ice road surface, or Raining/Fog weather
    df_flagged = df_sev.withColumn(
        "Is_Adverse_Condition",
        F.when(
            F.col("Time_Period").isin("Night", "Late Night") |
            F.col("Road_Surface_Conditions").isin("Wet or Damp", "Snow/Ice", "Flood over 3cm deep") |
            F.col("Weather_Conditions").isin("Raining no high winds", "Raining + high winds", "Fog or mist"),
            1
        ).otherwise(0)
    )

    # 2. Aggregate location raw metrics
    location_raw = (
        df_flagged.groupBy("Local_Authority_District")
        .agg(
            F.count("*").alias("Frequency"),
            F.sum("Severity_Weight").alias("Severity_Score"),
            F.sum("Is_Adverse_Condition").alias("Adverse_Accidents_Count"),
            F.sum(F.when(F.col("Accident_Severity") == "Fatal", 1).otherwise(0)).alias("Fatal_Count"),
            F.sum(F.when(F.col("Accident_Severity") == "Serious", 1).otherwise(0)).alias("Serious_Count")
        )
        .withColumn("Adverse_Share", F.col("Adverse_Accidents_Count") / F.col("Frequency"))
    )

    # 3. Compute Min and Max stats for normalization
    stats = location_raw.select(
        F.min("Frequency").alias("min_freq"),
        F.max("Frequency").alias("max_freq"),
        F.min("Severity_Score").alias("min_sev"),
        F.max("Severity_Score").alias("max_sev"),
        F.min("Adverse_Share").alias("min_adv"),
        F.max("Adverse_Share").alias("max_adv")
    ).collect()[0]

    min_f, max_f = stats["min_freq"], stats["max_freq"]
    min_s, max_s = stats["min_sev"], stats["max_sev"]
    min_a, max_a = stats["min_adv"], stats["max_adv"]

    # Avoid division by zero
    range_f = max_f - min_f if max_f > min_f else 1.0
    range_s = max_s - min_s if max_s > min_s else 1.0
    range_a = max_a - min_a if max_a > min_a else 1.0

    # 4. Apply Min-Max Normalization and Composite Formula
    # Formula: Risk_Score = (0.40 * Norm_Sev + 0.35 * Norm_Freq + 0.25 * Norm_Adv) * 100
    risk_scored_df = (
        location_raw
        .withColumn("Norm_Frequency", (F.col("Frequency") - min_f) / range_f)
        .withColumn("Norm_Severity", (F.col("Severity_Score") - min_s) / range_s)
        .withColumn("Norm_Adverse", (F.col("Adverse_Share") - min_a) / range_a)
        .withColumn(
            "Composite_Risk_Score",
            F.round(
                ((0.40 * F.col("Norm_Severity")) +
                 (0.35 * F.col("Norm_Frequency")) +
                 (0.25 * F.col("Norm_Adverse"))) * 100,
                2
            )
        )
        .orderBy(F.desc("Composite_Risk_Score"))
    )

    print("--- RANKED LIST OF HIGH-RISK LOCATIONS (COMPOSITE RISK SCORE) ---")
    risk_scored_df.select(
        "Local_Authority_District", "Frequency", "Severity_Score",
        "Fatal_Count", "Adverse_Share", "Composite_Risk_Score"
    ).show(truncate=False)

    model_justification = f"""
================================================================================
 COMPOSITE ROAD SAFETY RISK SCORE METHODOLOGY & JUSTIFICATION
================================================================================
 Score Formula:
   Risk_Score = [ 0.40 * Norm(Severity_Score) +
                  0.35 * Norm(Accident_Frequency) +
                  0.25 * Norm(Adverse_Condition_Share) ] * 100

 Component Weight Justification:
 1. Severity Score Weight (40%):
    Directly reflects human loss and injury burden (Fatal=5, Serious=3, Slight=1).
    Heaviest weight ensures resource allocation prioritizes life-saving areas.

 2. Accident Frequency Weight (35%):
    Measures baseline spatial crash density and traffic collision risk volume.

 3. Adverse Condition Share Weight (25%):
    Measures location vulnerability to darkness, rain, and adverse weather conditions.

 Data Bounds:
   - Frequency Range: [{min_f} .. {max_f}]
   - Severity Score Range: [{min_s} .. {max_s}]
   - Adverse Share Range: [{min_a:.4f} .. {max_a:.4f}]
================================================================================
"""
    print(model_justification)

    output_dir = os.path.join("output", "task7_risk_score")
    os.makedirs(output_dir, exist_ok=True)
    risk_scored_df.write.mode("overwrite").csv(
        os.path.join(output_dir, "ranked_location_risk_scores.csv"), header=True
    )
    
    with open(os.path.join(output_dir, "risk_model_methodology.txt"), "w", encoding="utf-8") as f:
        f.write(model_justification)

    print(f"[Task 7] Composite risk score model complete. Written to: {output_dir}")
    print("==========================================================\n")

    if own_spark:
        spark.stop()

    return risk_scored_df


if __name__ == "__main__":
    run()
