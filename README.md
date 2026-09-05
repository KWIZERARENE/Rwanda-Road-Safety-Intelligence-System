# Rwanda Road Safety Intelligence System (RRSIS)
## HDFS + Apache Spark (PySpark DataFrame API) Analytics Engine

### 1. Overview & Rwanda Case Study Context
The **Rwanda Road Safety Intelligence System (RRSIS)** is an enterprise big data analytics engine engineered to ingest, clean, analyze, and visualize high-volume road accident records to generate data-driven safety intelligence for road safety authorities.

Sourced against statistical benchmarks from the **National Institute of Statistics of Rwanda (NISR) Statistical Yearbook 2024** (*Table 14.2.6: Road Accidents*, recording **9,995 total accidents in 2023** and **761 fatal crashes**), RRSIS leverages the **Hadoop Distributed File System (HDFS)** for scalable, fault-tolerant storage and **Apache Spark (PySpark DataFrame API)** for in-memory distributed analytics.

> **Dataset Transparency Disclosure**: In accordance with the project specification, the Kaggle Road Accident Dataset (307,973 raw crash records, 23 attributes) serves as an **enterprise surrogate dataset** for developing and validating the PySpark analytics engine. Official Rwandan macro-statistics from NISR are integrated for national context, and a roadmap for ingesting actual Rwanda National Police (RNP) accident micro-data upon release is provided.

---

### 2. Repository Directory Structure
The repository is structured into core directories housing the master notebook, technical reports, presentation decks, and visual artifacts:

```
Midterm1_repo/
├── README.md                          # Project overview, setup & execution guide
├── requirements.txt                   # PySpark, pandas, python-docx, python-pptx, matplotlib, seaborn
├── hdfs_setup_commands.sh             # HDFS directory creation and dataset upload script
│
├── docs/                              # Technical reports & executive presentation slide decks
│   ├── Technical_Report.docx          # Formatted Microsoft Word Technical Report (.docx)
│   ├── Technical_Report.md            # Comprehensive Academic Standard Technical Report (Markdown)
│   ├── Code_Explanation_Guide.md      # Detailed line-by-line PySpark code guide & Viva-Voce Q&A
│   ├── Final_Presentation.pptx        # Executive PowerPoint Slide Deck (.pptx)
│   └── Final_Presentation.md          # Markdown Presentation Deck
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
    └── RRSIS_Full_Analysis.ipynb      # Self-contained master notebook covering Tasks 1–10
```

---

### 3. Setup & Ingestion Instructions

#### Step 1: Install Python Dependencies
```bash
pip install -r requirements.txt
```

#### Step 2: Start Hadoop HDFS Service
```bash
# Start NameNode and DataNode services
start-dfs.sh

# Verify HDFS status
hdfs dfsadmin -report
```

#### Step 3: Execute HDFS Ingestion Script
```bash
chmod +x hdfs_setup_commands.sh
./hdfs_setup_commands.sh
```
Target HDFS URI:
`hdfs://localhost:9000/user/hadoop/rrsis/raw/road_accidents_23cols.csv`

---

### 4. Running the Master PySpark Notebook

Launch the master notebook in VS Code, JupyterLab, or Jupyter Notebook:
```bash
jupyter notebook notebooks/RRSIS_Full_Analysis.ipynb
```
The notebook automatically connects to PySpark, loads the dataset from HDFS (with automatic fallback to local dataset path if running offline), and executes Tasks 1 through 10.

---

### 5. Detailed Task Summary & Analytical Scope

| Task | Analytical Scope | Key PySpark DataFrame Operations & Empirical Outputs |
|---|---|---|
| **Task 1** | HDFS Data Ingestion | Direct HDFS read, schema inference, total count (**307,973 records, 23 attributes**), partition load balancing via `spark_partition_id()` across **8 RDD partitions**. |
| **Task 2** | Data Quality Engineering | **Explicit Data Audit BEFORE Cleaning**: Tabulates missing counts per column (`Carriageway_Hazards`: 302,549; `Weather_Conditions`: 6,057; `Road_Type`: 1,534; `Road_Surface_Conditions`: 317; `Time`: 17), duplicate rows (1), non-positive speed limits (0), zero coordinates (0), and typos (`Fetal` -> `Fatal`). Executes `initcap`, `trim`, `dropDuplicates()`, categorical `"Unknown"` imputation, range nullification, and `cache()`. |
| **Task 3** | Temporal Intelligence | Chronological aggregations by **Hour of Day** (00–23), **Day of Week** (Friday peak: 50,529 / 16.41%), **Month** (Nov peak: 29,095 / 9.45%), **Weekday vs Weekend** (75.61% vs 24.39%), and **5 Custom Time Periods** (`Afternoon`: 34.23%, `Morning`: 28.69%, `Evening`: 24.44%, `Night`: 7.79%, `Late Night`: 4.83%, **Highest Avg Severity Weight = 1.52**). |
| **Task 4** | Accident Severity Index | Weighted Severity Score ($	ext{Slight}=1, 	ext{Serious}=3, 	ext{Fatal}=5$). Calculates severity across **Location**, **Road Type** (Single Carriageway: 230,611 crashes / 76.0% severity burden), **Vehicle Type**, and **Time Period**. Proves **Frequency vs. Severity Risk Divergence** (Westminster Avg Severity Weight = 1.54 vs Birmingham 1.27). |
| **Task 5** | Dangerous Factor Combinations | Multi-attribute grouping across Road Type, Speed Limit, Weather, Light, Time Period, and Vehicle Type. Ranks Top 10 dangerous tuples (60 mph Single Carriageways elevate per-crash severity weight to 1.56–1.61). |
| **Task 6** | Window Location Ranking | Applies `Window.partitionBy("Urban_or_Rural_Area").orderBy(F.desc("Severity_Score"))` evaluating `row_number()`, `rank()`, and `dense_rank()`. Displays Top 3 Rural (1. Cornwall, 2. County Durham, 3. Wiltshire) and Top 3 Urban (1. Birmingham, 2. Westminster, 3. Leeds). |
| **Task 7** | Composite Risk Score | Formulates 0–100 Composite Risk Score combining Normalized Severity (40%), Normalized Frequency (35%), and Normalized Adverse Share (25%) using Min-Max scaling ($N_X = (X - X_{\min}) / (X_{\max} - X_{\min})$). Ranks top locations (1. Birmingham: 88.54, 2. Leeds: 63.40, 3. Westminster: 54.16, 4. Manchester: 51.84, 5. Bradford: 48.95). |
| **Task 8** | Spark Execution Analysis | Runs `df.explain(True)` output analyzing Parsed, Analyzed, Optimized, and Physical Catalyst execution plans. Identifies wide shuffle operations (`Exchange hashpartitioning`) and benchmarks `cache()` optimization (**11.2x execution time speedup**). |
| **Task 9** | Management Challenge | Formulates 5 prioritized road safety interventions strictly following $	ext{Data} ightarrow 	ext{Spark Analysis} ightarrow 	ext{Evidence} ightarrow 	ext{Recommendation}$. |
| **Task 10**| Geospatial Mapping & Visualizations | 2D coordinate scatterplot mapping (`Latitude` vs `Longitude` color-coded by severity) and automated chart generation. |

---

### 6. Project Technical Deliverables

- **Master PySpark Notebook**: [`notebooks/RRSIS_Full_Analysis.ipynb`](notebooks/RRSIS_Full_Analysis.ipynb)
- **Technical Report (Word .docx)**: [`docs/Technical_Report.docx`](docs/Technical_Report.docx)
- **Technical Report (Markdown)**: [`docs/Technical_Report.md`](docs/Technical_Report.md)
- **Code Explanation & Defense Guide**: [`docs/Code_Explanation_Guide.md`](docs/Code_Explanation_Guide.md)
- **Executive PowerPoint Presentation**: [`docs/Final_Presentation.pptx`](docs/Final_Presentation.pptx)
- **Markdown Presentation Slide Deck**: [`docs/Final_Presentation.md`](docs/Final_Presentation.md)
- **Output Cartographic Figures**: [`output/figures/`](output/figures/)

---

### 7. Project Group Credentials

- **101405 AMIE MARIE FLORA DUSHIMUMUKIZA** — Data Quality Engineering, Window Ranking Models & Report Synthesis
- **101379 KWIZERA RENE** — HDFS Architecture, Spark Ingestion, Catalyst Execution & Performance Analysis
- **101378 GRACE TETA** — Temporal Intelligence, Severity Modeling, Factor Combinations & Management Strategy
