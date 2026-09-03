#!/bin/bash
# ==============================================================================
# RRSIS Convenience Wrapper: Upload Local Dataset to HDFS
# ==============================================================================

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( dirname "${SCRIPT_DIR}" )"

cd "${PROJECT_ROOT}"

echo "Project Root Directory: ${PROJECT_ROOT}"
echo "Running HDFS upload wrapper..."

if [ -f "hdfs_setup_commands.sh" ]; then
    bash hdfs_setup_commands.sh
else
    echo "ERROR: hdfs_setup_commands.sh not found in project root."
    exit 1
fi
