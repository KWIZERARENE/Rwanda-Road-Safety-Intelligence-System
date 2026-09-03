"""
RRSIS Pipeline Orchestrator (Tasks 1 to 10)
-------------------------------------------
Executes the full end-to-end Rwanda Road Safety Intelligence System pipeline
using PySpark DataFrames, HDFS dataset ingestion, and Geospatial Map Visualization.

Run pipeline:
    python src/run_pipeline.py
"""

import os
import sys
import time

# Ensure repository root and src directory are in sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

try:
    from src.spark_session import get_spark_session
    import src.task1_ingestion as task1
    import src.task2_data_quality as task2
    import src.task3_temporal_analysis as task3
    import src.task4_severity_index as task4
    import src.task5_factor_combinations as task5
    import src.task6_window_ranking as task6
    import src.task7_risk_score as task7
    import src.task8_performance_analysis as task8
    import src.task9_recommendations as task9
    import src.visualization as task10
except ImportError:
    from spark_session import get_spark_session
    import task1_ingestion as task1
    import task2_data_quality as task2
    import task3_temporal_analysis as task3
    import task4_severity_index as task4
    import task5_factor_combinations as task5
    import task6_window_ranking as task6
    import task7_risk_score as task7
    import task8_performance_analysis as task8
    import task9_recommendations as task9
    import visualization as task10


def main():
    start_time = time.time()
    print("==================================================================")
    print("   RWANDA ROAD SAFETY INTELLIGENCE SYSTEM (RRSIS)")
    print("   PySpark + HDFS Big Data Analytics Pipeline Orchestrator")
    print("==================================================================")

    # Initialize shared SparkSession
    spark = get_spark_session("RRSIS_Pipeline_Orchestrator")
    print(f"Active PySpark Version: {spark.version}\n")

    # Task 1: Ingestion & Partition Diagnostics
    df_raw = task1.run(spark)

    # Task 2: Data Quality & Sanitization
    df_clean = task2.run(spark, df_raw)
    
    # Apply PySpark Cache optimization to avoid redundant HDFS reads across downstream tasks
    print("[Orchestrator Optimization] Caching sanitized DataFrame df_clean in memory...")
    df_clean.cache()

    # Task 3: Temporal Analysis
    df_temp = task3.run(spark, df_clean)

    # Task 4: Severity Index Analysis
    df_sev = task4.run(spark, df_temp)

    # Task 5: Factor Combinations Analysis
    _ = task5.run(spark, df_sev)

    # Task 6: Window Function Rankings
    _ = task6.run(spark, df_sev)

    # Task 7: Composite Risk Score Model
    risk_scored_df = task7.run(spark, df_sev)

    # Task 8: Performance Analysis & Explain Plan
    _ = task8.run(spark, df_sev)

    # Task 9: Final Management Challenge Recommendations
    task9.run(spark)

    # Task 10: Geospatial Mapping & Advanced Data Visualization
    task10.run(spark, df_sev, risk_scored_df)

    elapsed_time = time.time() - start_time
    print("==================================================================")
    print(f" RRSIS PIPELINE COMPLETED SUCCESSFULLY IN {elapsed_time:.2f} SECONDS")
    print(" Output CSV, PNG charts, and HTML maps generated in 'output/' directory.")
    print("==================================================================")

    # Clean up SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
