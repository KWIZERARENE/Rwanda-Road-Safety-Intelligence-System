# Rwanda Road Safety Intelligence System (RRSIS)
## HDFS + Apache Spark (PySpark) Analytics Engine

### 1. Overview
The **Rwanda Road Safety Intelligence System (RRSIS)** is an enterprise-grade big data analytics pipeline developed to process, analyze, and extract actionable road-safety intelligence from high-volume accident data. 

Sourced against statistical benchmarks from the **National Institute of Statistics of Rwanda (NISR) Statistical Yearbook 2024** (Table 14.2.6: 9,995 total accidents, 761 fatal accidents in 2023), RRSIS leverages **Hadoop Distributed File System (HDFS)** for distributed storage and **Apache Spark (PySpark DataFrame API)** for high-performance distributed analytics.

---

### 2. Repository Directory Structure
The repository is strictly structured into 3 core directories for maximum clarity and ease of evaluation:
```
Midterm1_repo/
├── README.md                          # Project overview, setup & instructions
├── requirements.txt                   # pyspark, pandas, python-docx, matplotlib, seaborn
├── hdfs_setup_commands.sh             # HDFS directory creation and dataset upload script
│
├── docs/                              # Technical reports & presentation guides
│   ├── Technical_Report.docx          # 15-Page Academic Standard Technical Report (Word .docx)
│   ├── Technical_Report.md            # Comprehensive Markdown Technical Report
│   └── Code_Explanation_Guide.md      # Detailed line-by-line PySpark code guide & viva-voce prep
│
├── output/                            # Output figures & cartographic graphics
│   └── figures/                       # High-resolution PNG plots generated directly from code
│       ├── geographical_accident_hotspots_map.png
│       ├── corridor_risk_and_heatmaps.png
│       ├── temporal_accident_intelligence.png
│       ├── top10_dangerous_factor_combinations.png
│       ├── road_safety_risk_score_model.png
│       └── spark_dag_execution_architecture.png
│
└── notebooks/                         # Master PySpark Analytics Notebook
    └── RRSIS_Full_Analysis.ipynb      # The single, self-contained notebook covering Tasks 1–10
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

The entire end-to-end RRSIS analytics workload (Tasks 1 through 10) is fully consolidated within the master Jupyter Notebook.

#### Launching the Master Notebook
```bash
jupyter notebook notebooks/RRSIS_Full_Analysis.ipynb
```
Or open directly within VS Code or JupyterLab. The notebook reads the dataset directly from Hadoop HDFS:
`hdfs://localhost:9000/road_safety_dataset/Road Accident Data.csv`

---

### 5. Task Summary & Analytical Scope in Master Notebook

| Task | Scope in Notebook | Key PySpark Operations & Outputs |
|---|---|---|
| **Task 1** | HDFS Data Ingestion | Direct HDFS read, `printSchema()`, `describe().show()`, partition diagnostics via `spark_partition_id()`. |
| **Task 2** | Data Quality Engineering | Audit missing values, deduplicate records, fix typos (`Fetal`->`Fatal`), impute `Unknown`, nullify zero coordinates, and call `cache()`. |
| **Task 3** | Temporal Intelligence | Hourly crash curves, weekday vs. weekend risk, and 5 custom time periods (`Late Night`, `Morning`, `Afternoon`, `Evening`, `Night`). |
| **Task 4** | Accident Severity Index | Weighted Severity Score ($\text{Slight}=1, \text{Serious}=3, \text{Fatal}=5$); frequency vs severity risk divergence. |
| **Task 5** | Dangerous Factor Combinations | Multi-attribute grouping (Road Type, Speed Limit, Weather, Light, Time Period) and Top 10 dangerous combinations. |
| **Task 6** | Window Location Rankings | Top-3 highest-risk locations in Urban vs Rural categories using `Window.partitionBy()` and `row_number()`, `rank()`, `dense_rank()`. |
| **Task 7** | Composite Risk Score | 0–100 Multi-dimensional Composite Risk Score combining normalized Severity (40%), Frequency (35%), and Adverse Conditions (25%). |
| **Task 8** | Spark Execution Analysis | Catalyst plan analysis via `df.explain(True)`, Action vs Job vs Stage vs Task mapping, Shuffle causes, and `cache()` benchmarking. |
| **Task 9** | 5 Management Priorities | 5 actionable priorities following $\text{Data} \rightarrow \text{Spark Analysis} \rightarrow \text{Evidence} \rightarrow \text{Recommendation}$. |
| **Task 10**| Geospatial Map & Defense | Inline 2D coordinate scatterplot color-coded by severity and complete Viva-Voce oral defense guide. |

---

### 6. Technical Deliverables (Exam Rubric Alignment)
- **15-Page Standard Technical Report (Word)**: [`docs/Technical_Report.docx`](docs/Technical_Report.docx) (4.05 MB formatted document with all empirical figures embedded)
- **Technical Report (Markdown)**: [`docs/Technical_Report.md`](docs/Technical_Report.md)
- **Code Explanation & Defense Guide**: [`docs/Code_Explanation_Guide.md`](docs/Code_Explanation_Guide.md)
- **Master PySpark Notebook**: [`notebooks/RRSIS_Full_Analysis.ipynb`](notebooks/RRSIS_Full_Analysis.ipynb)
- **Output Empirical Figures**: [`output/figures/`](output/figures/)

#### Project Group Members:
- **101405 AMIE MARIE FLORA DUSHIMUMUKIZA**
- **101379 KWIZERA RENE**
- **101378 GRACE TETA**
