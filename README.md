# Rwanda Road Safety Intelligence System (RRSIS)
## HDFS + Apache Spark (PySpark) Analytics Engine

### 1. Overview
The **Rwanda Road Safety Intelligence System (RRSIS)** is an enterprise-grade big data analytics pipeline developed to process, analyze, and extract actionable road-safety intelligence from high-volume accident data. 

Sourced against statistical benchmarks from the **National Institute of Statistics of Rwanda (NISR) Statistical Yearbook 2024** (Table 14.2.6: 9,995 total accidents, 761 fatal accidents in 2023), RRSIS leverages **Hadoop Distributed File System (HDFS)** for distributed storage and **Apache Spark (PySpark DataFrame API)** for high-performance distributed analytics.

---

### 2. Repository Directory Structure
```
rrsis/
├── README.md                          # Project overview, setup & run instructions
├── requirements.txt                   # pyspark, pandas, python-docx, etc.
├── hdfs_setup_commands.sh             # HDFS mkdir/put shell commands (Task 1)
│
├── data/
│   ├── raw/
│   │   └── road_accidents_23cols.csv  # Kaggle surrogate dataset (23 columns)
│   └── hdfs_stage/                    # Local staging copy before `hdfs dfs -put`
│
├── src/
│   ├── __init__.py
│   ├── spark_session.py               # Shared SparkSession builder + HDFS config
│   ├── task1_ingestion.py             # HDFS + Spark data ingestion
│   ├── task2_data_quality.py          # Missing values, duplicates, cleaning
│   ├── task3_temporal_analysis.py     # Hour/day/month/weekday risk periods
│   ├── task4_severity_index.py        # Weighted Severity Score Index
│   ├── task5_factor_combinations.py   # Dangerous factor-combination ranking
│   ├── task6_window_ranking.py        # Top-3 per category via Window functions
│   ├── task7_risk_score.py            # Composite Road Safety Risk Score
│   ├── task8_performance_analysis.py  # explain(), shuffles, cache() analysis
│   ├── task9_recommendations.py       # Final 5 management priorities
│   └── run_pipeline.py                # Orchestrates Tasks 1–9 end-to-end
│
├── notebooks/
│   └── RRSIS_Full_Analysis.ipynb      # Presentation-friendly notebook version
│
├── scripts/
│   ├── upload_to_hdfs.sh              # Convenience wrapper for HDFS upload
│   └── generate_docx_report.py        # Script to generate Technical_Report.docx
│
├── output/                            # Spark write() CSV/Parquet results per task
│   ├── task1_ingestion/
│   ├── task2_data_quality/
│   ├── task3_temporal/
│   ├── task4_severity/
│   ├── task5_factor_combinations/
│   ├── task6_window_ranking/
│   ├── task7_risk_score/
│   ├── task8_performance/
│   ├── task9_recommendations/
│   └── screenshots/                   # HDFS UI, Spark UI, terminal evidence
│
└── docs/
    ├── Technical_Report.md            # Markdown technical report
    └── Technical_Report.docx          # Word document technical report
```

---

### 3. Setup & HDFS Ingestion Instructions

#### Step 1: Install Python Dependencies
```bash
pip install -r requirements.txt
```

#### Step 2: Start Hadoop / HDFS Service
```bash
# Start NameNode and DataNode services
start-dfs.sh
# Verify HDFS status
hdfs dfsadmin -report
```

#### Step 3: Execute HDFS Setup Script
```bash
chmod +x hdfs_setup_commands.sh
./hdfs_setup_commands.sh
```
Or execute manually:
```bash
hdfs dfs -mkdir -p /user/hadoop/rrsis/raw
hdfs dfs -mkdir -p /user/hadoop/rrsis/output
hdfs dfs -put -f data/raw/road_accidents_23cols.csv /user/hadoop/rrsis/raw/
hdfs dfs -ls /user/hadoop/rrsis/raw
```

---

### 4. Running the Analytics Pipeline

#### Option A: End-to-End Orchestration (Tasks 1–9)
```bash
python src/run_pipeline.py
```

#### Option B: Individual Task Execution
```bash
python src/task1_ingestion.py
python src/task2_data_quality.py
python src/task3_temporal_analysis.py
python src/task4_severity_index.py
python src/task5_factor_combinations.py
python src/task6_window_ranking.py
python src/task7_risk_score.py
python src/task8_performance_analysis.py
python src/task9_recommendations.py
```

#### Option C: Interactive Jupyter Notebook
```bash
jupyter notebook notebooks/RRSIS_Full_Analysis.ipynb
```

---

### 5. Task Summary & Analytical Scope

| Task | Module | Key Operations & Deliverables |
|---|---|---|
| **Task 1** | `task1_ingestion.py` | Load dataset from `hdfs://localhost:9000/...`, display schema, total count, column metadata. |
| **Task 2** | `task2_data_quality.py` | Audit missing values, deduplicate records, fix typos ('Fetal'->'Fatal'), impute 'Unknown', nullify zero coordinates. |
| **Task 3** | `task3_temporal_analysis.py` | Aggregate by Hour, Day of Week, Month, Weekend vs Weekday, and custom Time Periods (Night, Morning, Afternoon, Evening, Late Night). |
| **Task 4** | `task4_severity_index.py` | Calculate Weighted Severity Score ($\text{Slight}=1, \text{Serious}=3, \text{Fatal}=5$) across locations, road types, and vehicle types. |
| **Task 5** | `task5_factor_combinations.py` | Group by multi-factor tuples (Road Type, Speed Limit, Weather, Light, Time Period) and rank Top 10 dangerous combinations. |
| **Task 6** | `task6_window_ranking.py` | Compute Top-3 highest-risk locations within every geographic category using Spark `Window.partitionBy()` and `row_number()`, `rank()`, `dense_rank()`. |
| **Task 7** | `task7_risk_score.py` | Build normalized Composite Road Safety Risk Score (0–100 scale) combining normalized frequency, severity score, and adverse condition shares. |
| **Task 8** | `task8_performance_analysis.py` | Execute `df.explain(True)`, analyze physical execution stages, wide/narrow transformations, shuffle causes, and `cache()` performance. |
| **Task 9** | `task9_recommendations.py` | Define 5 actionable management priorities following `Data → Spark Analysis → Evidence → Recommendation`. |

---

### 6. Technical Report & Deliverables
- **Technical Report (Word)**: `docs/Technical_Report.docx`
- **Technical Report (Markdown)**: `docs/Technical_Report.md`
- **Report Generator Script**: `python scripts/generate_docx_report.py`
