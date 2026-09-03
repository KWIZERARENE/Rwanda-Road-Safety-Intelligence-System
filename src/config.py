"""
RRSIS Central HDFS & Pipeline Configuration File
------------------------------------------------
Edit the HDFS_DATASET_PATH below whenever your HDFS dataset directory or filename changes.
All PySpark ingestion modules and tasks import dataset settings from this single file.
"""

# ------------------------------------------------------------------------------
# HDFS DATASET PATH CONFIGURATION
# ------------------------------------------------------------------------------
# Change this path to your target HDFS directory / CSV file:
HDFS_DATASET_PATH = "hdfs://localhost:9000/user/hadoop/my_dataset/Road Accident Data.csv"

# Alternative HDFS directory paths (for reference):
# HDFS_DATASET_PATH = "hdfs://localhost:9000/user/hadoop/rrsis/raw/road_accidents_23cols.csv"

# ------------------------------------------------------------------------------
# SPARK ENGINE CONFIGURATION
# ------------------------------------------------------------------------------
SPARK_APP_NAME = "RRSIS_Analytics_Engine"
SPARK_MASTER = "local[*]"
SPARK_SHUFFLE_PARTITIONS = "8"
SPARK_DRIVER_MEMORY = "2g"
SPARK_UI_PORT = "4050"
