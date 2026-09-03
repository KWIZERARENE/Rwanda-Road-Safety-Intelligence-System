"""
RRSIS SparkSession Manager
--------------------------
Provides a shared PySpark SparkSession configured using central settings from src/config.py.
"""

import sys
import os
from pyspark.sql import SparkSession

try:
    from src.config import SPARK_APP_NAME, SPARK_MASTER, SPARK_SHUFFLE_PARTITIONS, SPARK_DRIVER_MEMORY, SPARK_UI_PORT
except ImportError:
    from config import SPARK_APP_NAME, SPARK_MASTER, SPARK_SHUFFLE_PARTITIONS, SPARK_DRIVER_MEMORY, SPARK_UI_PORT


def get_spark_session(app_name=None, master=None):
    """
    Builds or retrieves an existing PySpark SparkSession using central config.
    """
    if app_name is None:
        app_name = SPARK_APP_NAME
    if master is None:
        master = SPARK_MASTER

    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS) \
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
        .config("spark.ui.port", SPARK_UI_PORT) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
        
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


if __name__ == "__main__":
    spark = get_spark_session("RRSIS_Session_Test")
    print("==================================================")
    print("SparkSession initialized successfully!")
    print(f"  App Name: {spark.sparkContext.appName}")
    print(f"  Version:  {spark.version}")
    print(f"  Master:   {spark.sparkContext.master}")
    print("==================================================")
    spark.stop()
