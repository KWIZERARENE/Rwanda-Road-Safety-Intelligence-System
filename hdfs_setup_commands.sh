#!/bin/bash
# ==============================================================================
# HDFS Setup Commands for Rwanda Road Safety Intelligence System (RRSIS)
# Task 1: HDFS + Spark Data Ingestion Setup
# ==============================================================================

echo "=================================================================="
echo "      RRSIS: HDFS DIRECTORY & DATASET SETUP"
echo "=================================================================="

# 1. Define HDFS Base Directories
HDFS_RAW_DIR="/user/hadoop/rrsis/raw"
HDFS_OUTPUT_DIR="/user/hadoop/rrsis/output"
LOCAL_DATA_PATH="data/raw/road_accidents_23cols.csv"

# 2. Check HDFS Service Availability
echo "[1] Checking HDFS availability..."
hdfs dfsadmin -report > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "ERROR: HDFS service is not reachable. Ensure Hadoop NameNode is started."
    echo "Command to start Hadoop: start-dfs.sh"
    exit 1
fi
echo "    [OK] HDFS is running."

# 3. Create HDFS Project Directories
echo "[2] Creating HDFS directories..."
hdfs dfs -mkdir -p ${HDFS_RAW_DIR}
hdfs dfs -mkdir -p ${HDFS_OUTPUT_DIR}
echo "    [OK] Directory created: ${HDFS_RAW_DIR}"
echo "    [OK] Directory created: ${HDFS_OUTPUT_DIR}"

# 4. Stage & Upload Dataset to HDFS
echo "[3] Uploading Kaggle Road Accident Dataset to HDFS..."
if [ -f "${LOCAL_DATA_PATH}" ]; then
    hdfs dfs -put -f ${LOCAL_DATA_PATH} ${HDFS_RAW_DIR}/road_accidents_23cols.csv
    echo "    [OK] File uploaded successfully to ${HDFS_RAW_DIR}/road_accidents_23cols.csv"
else
    echo "WARNING: Local file '${LOCAL_DATA_PATH}' not found."
    echo "Please ensure the dataset file exists before uploading."
fi

# 5. Verify HDFS Storage Contents
echo "[4] Verifying HDFS storage contents:"
hdfs dfs -ls ${HDFS_RAW_DIR}

echo "=================================================================="
echo " HDFS Setup Complete. Target HDFS URI:"
echo " hdfs://localhost:9000/user/hadoop/rrsis/raw/road_accidents_23cols.csv"
echo "=================================================================="
