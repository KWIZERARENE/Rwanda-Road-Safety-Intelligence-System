# RWANDA ROAD SAFETY INTELLIGENCE SYSTEM (RRSIS)
## COMPREHENSIVE TECHNICAL REPORT & ARCHITECTURAL DOCUMENTATION
**HDFS + Apache Spark (PySpark DataFrame API) Distributed Analytics Engine**

---

### MID-TERM GROUP PROJECT DELIVERABLE
**Institution**: College of Science & Technology, University of Rwanda / Carnegie Mellon University Africa  
**Department**: Computer & Information Engineering / Data Science & Analytics  
**Course Module**: Distributed Systems & Big Data Analytics (HDFS & Apache Spark)  
**Academic Year**: 2025/2026  

#### PROJECT GROUP MEMBERS (TEAM CREDENTIALS):
| Student Registration ID | Full Legal Name | Primary Role & System Responsibilities |
| :--- | :--- | :--- |
| **101405** | **AMIE MARIE FLORA DUSHIMUMUKIZA** | Data Quality Engineering, Window Ranking Models & Report Synthesis |
| **101379** | **KWIZERA RENE** | HDFS Architecture, Spark Ingestion, Catalyst Execution & Performance Analysis |
| **101378** | **GRACE TETA** | Temporal Intelligence, Severity Modeling, Factor Combinations & Management Strategy |

---

### TABLE OF CONTENTS
1. **Executive Summary & Rwandan Case Study**
   - 1.1 Problem Statement & Background
   - 1.2 National Institute of Statistics of Rwanda (NISR) Official Benchmark Data (2018–2023)
   - 1.3 System Architectural Topology (HDFS Storage & Spark Compute Engine)
2. **Task 1: HDFS Storage & PySpark Data Ingestion**
   - 2.1 HDFS Cluster Directory Setup & Block Layout
   - 2.2 Spark Distributed Reading & Schema Inference
   - 2.3 RDD Partition Diagnostics & Locality Analysis
3. **Task 2: Data Quality Engineering & Sanitization Pipeline**
   - 3.1 Systematic Audit of Data Quality Anomalies BEFORE Cleaning
   - 3.2 Justified Transformation Pipeline (Typo Correction, Key Deduplication, Spatial Nullification)
   - 3.3 Analytical Consequences of Naive Row Deletion vs Imputation
4. **Task 3: Temporal Accident Intelligence & Dynamics**
   - 4.1 Chronological Breakdown (Hourly, Day-of-Week, Monthly Seasonality, Weekday vs Weekend)
   - 4.2 Five Custom Time Period Windows (Late Night, Morning, Afternoon, Evening, Night)
   - 4.3 Identification of the Five Highest-Risk Time Periods with Numerical Proof
5. **Task 4: Accident Severity Index & Risk Divergence**
   - 5.1 Severity Score Formulation (Slight=1, Serious=3, Fatal=5)
   - 5.2 Multi-Dimensional Severity Aggregations (Location, Road Type, Vehicle Type, Time Period)
   - 5.3 Empirical Divergence: Why High Crash Frequency $\ne$ Greatest Safety Risk
6. **Task 5: Dangerous-Factor Combination Analysis**
   - 6.1 Multi-Attribute Feature Tuples
   - 6.2 Top 10 Most Dangerous Combinations Ranked by Severity Burden
7. **Task 6: Advanced Window-Based Location Ranking**
   - 7.1 Spark SQL Window Function Specifications (`partitionBy`, `orderBy`)
   - 7.2 Comparative Evaluation of `row_number()`, `rank()`, and `dense_rank()`
   - 7.3 Top 3 Highest-Risk Locations across Geographical Divisions (Urban vs Rural)
8. **Task 7: Multi-Dimensional Road Safety Risk Score Model**
   - 8.1 Mathematical Model Formulation & Component Normalization
   - 8.2 Justification of Composite Weighting (40% Severity, 35% Frequency, 25% Adverse Conditions)
   - 8.3 Ranked National Hotspot Prioritization Index
9. **Task 8: Spark Execution, Architecture & Performance Analysis**
   - 9.1 Catalyst Optimizer & `df.explain(True)` Execution Trees
   - 9.2 Transformations vs. Actions, Jobs, Stages, and Tasks
   - 9.3 Wide Dependencies & Network Shuffle Mechanics (`Exchange hashpartitioning`)
   - 9.4 Memory Optimization & `cache()` Evaluation
10. **Task 9: Final Management Challenge (5 Priority Policy Interventions)**
    - 10.1 Structured Policy Matrix (Data $\rightarrow$ Spark Analysis $\rightarrow$ Evidence $\rightarrow$ Recommendation)
11. **Rwandan Implementation Framework, Data Availability & Policy Roadmap**
    - 11.1 Surrogate Dataset Transparency vs Official NISR Micro-Data Integration
    - 11.2 Rwanda National Police (RNP) Digitization Timeline & Integration Framework
    - 11.3 Policy Recommendations for Rwanda Road Safety Authorities
12. **Conclusion & References**

---

### SECTION 1: EXECUTIVE SUMMARY & RWANDAN CASE STUDY

#### 1.1 Problem Statement & Background
Road traffic crashes represent an escalating public health crisis, socio-economic drain, and infrastructure challenge across developing nations, and Rwanda in particular. Over the past decade, Rwanda has experienced rapid economic expansion, urbanization, and vehicular fleet growth. However, this growth has coincided with severe roadway trauma. Anecdotal or manually compiled monthly crash logs have historically led to reactive, fragmented, and sub-optimal police deployment.

To establish an intelligence-driven, proactive road safety apparatus, this project presents the **Rwanda Road Safety Intelligence System (RRSIS)**. Engineered upon the **Hadoop Distributed File System (HDFS)** for resilient, distributed storage and **Apache Spark (PySpark DataFrame API)** as the in-memory distributed analytics engine, RRSIS ingests high-volume crash records to empirically answer:
*Where, when, and under what specific combinations of physical conditions is accident severity highest, and how should national resources be prioritized?*

#### 1.2 Official Rwandan Benchmark Data: NISR Statistical Yearbook 2024
To ground the research in national reality, RRSIS benchmarks its findings against the official empirical accident figures published by the **National Institute of Statistics of Rwanda (NISR)** in collaboration with the **Rwanda National Police (RNP)** (*Statistical Yearbook 2024, Chapter 14: Transport and Communication, Table 14.2.6: Road Accidents*):

```
+---------------------------------------------------------------------------------------------------+
| TABLE 14.2.6: ROAD ACCIDENTS IN RWANDA (2018 - 2023) - NISR STATISTICAL YEARBOOK 2024             |
+---------------------------------------------------------------------------------------------------+
| Accident Category          |  2018   |  2019   |  2020   |  2021   |  2022   |  2023   | 6-Yr Total |
+----------------------------+---------+---------+---------+---------+---------+---------+------------+
| Fatal Accidents            |   597   |   673   |   675   |   621   |   676   |   761   |    4,003   |
| Serious Injury Accidents   |   885   |   911   |   710   |   471   |   110   |   262   |    3,349   |
| Minor Injury Accidents     |  1,887  |  1,485  |  1,326  |  3,688  |  5,186  |  5,037  |   18,609   |
| Property Damage Only       |  2,242  |  1,584  |  1,492  |  3,859  |  4,362  |  3,935  |   17,474   |
+----------------------------+---------+---------+---------+---------+---------+---------+------------+
| TOTAL RECORDED ACCIDENTS   |  5,611  |  4,653  |  4,203  |  8,639  | 10,334  |  9,995  |   43,435   |
+---------------------------------------------------------------------------------------------------+
Source: Rwanda National Police records published in NISR Statistical Yearbook 2024.
```

**Critical Observations from the NISR Official Benchmark**:
1. **Surge in Total Collisions**: Total recorded road crashes surged from **4,203 in 2020** to **9,995 in 2023**—an alarming increase of **+137.8%**.
2. **Record Fatal Trauma**: Fatal accidents reached an all-time peak of **761 deaths in 2023** (averaging over **2.08 fatal crashes every day**), representing an increase of **+12.6% over 2022** and **+27.5% over 2018**.
3. **Surrogate Dataset Acknowledgment**: As stipulated in the academic project requirements, the Kaggle Road Accident Dataset (307,973 raw crash records) is utilized as a surrogate dataset for engineering and evaluating the RRSIS distributed computing pipeline. We explicitly do not claim these raw rows represent actual Rwandan events, but use them to prove the scalable analytics engine while incorporating NISR figures for national policy translation.

---

### SECTION 2: TASK 1 - HDFS STORAGE & PYSPARK DATA INGESTION

#### 2.1 HDFS Storage Layer Topology
The RRSIS architecture decouples storage from compute:
- **Storage Subsystem**: Apache Hadoop Distributed File System (HDFS 3.3+).
- **HDFS Directory**: `/user/hadoop/rrsis/raw/` and `/user/hadoop/rrsis/output/`.
- **Fault Tolerance**: Standard 3x block replication across DataNodes.
- **Block Split Size**: 128 MB default block allocation.

```bash
# Production HDFS Setup Commands
hdfs dfs -mkdir -p /user/hadoop/rrsis/raw
hdfs dfs -mkdir -p /user/hadoop/rrsis/output
hdfs dfs -put -f data/raw/road_accidents_23cols.csv /user/hadoop/rrsis/raw/
hdfs dfs -ls /user/hadoop/rrsis/raw
```

#### 2.2 Spark Distributed Reading & Schema Inference
In Task 1, PySpark initializes a distributed `SparkSession` and ingests the dataset directly from HDFS:
```python
spark = SparkSession.builder     .appName("Rwanda_Road_Safety_Intelligence_System")     .config("spark.driver.memory", "4g")     .config("spark.sql.shuffle.partitions", "8")     .getOrCreate()

df_raw = spark.read     .option("header", "true")     .option("inferSchema", "true")     .csv("hdfs://localhost:9000/user/hadoop/rrsis/raw/road_accidents_23cols.csv")
```

- **Total Ingested Records**: `307,973` crash events.
- **Total Ingested Columns**: `23` attributes encompassing temporal, geographical, environmental, and vehicular variables.
- **Schema Breakdown**: Includes `Accident_Index`, `Accident_Date`, `Day_of_Week`, `Month`, `Year`, `Time`, `Latitude`, `Longitude`, `Accident_Severity`, `Local_Authority_District`, `Urban_or_Rural_Area`, `Road_Type`, `Speed_limit`, `Road_Surface_Conditions`, `Weather_Conditions`, `Light_Conditions`, `Vehicle_Type`, `Number_of_Casualties`, `Number_of_Vehicles`, `Police_Force`.

#### 2.3 RDD Partition Diagnostics
Using `df_raw.rdd.getNumPartitions()` and `F.spark_partition_id()`, the data ingestion task audited the cluster load balancing:
- **Total RDD Partitions**: `8` partitions.
- **Partition Balance**: Records are evenly distributed across the 8 partitions (~40,500 records per partition across partitions 0 through 6, and 22,870 records in partition 7).

---

### SECTION 3: TASK 2 - DATA QUALITY ENGINEERING & SANITIZATION PIPELINE

#### 3.1 Systematic Audit of Data Quality Anomalies BEFORE Cleaning
Prior to analytical aggregation, a comprehensive PySpark audit identified data quality flaws across `df_raw`:

```
+---------------------------------------------------------------------------------------------------+
| TASK 2: EMPIRICAL DATA QUALITY AUDIT BEFORE CLEANING (N = 307,973 RECORDS)                       |
+------------------------------------+-----------------------+-------------------+------------------+
| Attribute Column                   | Inferred Data Type    | Null / Bad Count  | Missing % Share  |
+------------------------------------+-----------------------+-------------------+------------------+
| Carriageway_Hazards                | String                | 302,549           | 98.24%           |
| Weather_Conditions                 | String                | 6,057             | 1.97%            |
| Road_Type                          | String                | 1,534             | 0.50%            |
| Road_Surface_Conditions            | String                | 317               | 0.10%            |
| Time                               | Timestamp/String      | 17                | 0.01%            |
| Accident_Severity ('Fetal' typo)   | String                | 12                | 0.004%           |
| Exact Duplicate Records            | Row Struct            | 1                 | 0.0003%          |
+------------------------------------+-----------------------+-------------------+------------------+
```

#### 3.2 Justified Transformation Pipeline
The cleaning pipeline executed the following operations using pure PySpark DataFrames:

```python
# 1. Header Standardization
for col in df_raw.columns:
    clean_col = col.replace(" ", "_").replace("(", "").replace(")", "")
    df_raw = df_raw.withColumnRenamed(col, clean_col)

# 2. Case Normalization & Whitespace Trimming
target_cat_cols = ["Accident_Severity", "Road_Type", "Weather_Conditions", "Road_Surface_Conditions", "Light_Conditions", "Urban_or_Rural_Area", "Local_Authority_District"]
cat_cols = [c for c in target_cat_cols if c in df_raw.columns]

df_clean = df_raw
for c in cat_cols:
    df_clean = df_clean.withColumn(c, F.initcap(F.trim(F.col(c).cast("string"))))

# 3. Typo Remediation ('Fetal' -> 'Fatal')
df_clean = df_clean.withColumn("Accident_Severity", F.when(F.col("Accident_Severity") == "Fetal", "Fatal").otherwise(F.col("Accident_Severity")))

# 4. Key Deduplication
df_clean = df_clean.dropDuplicates()

# 5. Missing Categorical Imputation
for c in cat_cols:
    df_clean = df_clean.withColumn(c, F.when(F.col(c).isNull() | (F.trim(F.col(c)) == "") | (F.col(c) == "None"), "Unknown").otherwise(F.col(c)))

# 6. Numeric & Coordinate Outlier Nullification
df_clean = df_clean.withColumn("Latitude", F.when(F.col("Latitude") == 0, None).otherwise(F.col("Latitude")))                    .withColumn("Longitude", F.when(F.col("Longitude") == 0, None).otherwise(F.col("Longitude")))                    .withColumn("Speed_limit", F.when(F.col("Speed_limit") <= 0, None).otherwise(F.col("Speed_limit")))

# 7. In-Memory Persistence
df_clean.cache()
```

#### 3.3 Analytical Consequences of Naive Row Deletion vs Imputation
A critical requirement in Task 2 is justifying why records with missing values were **imputed or sanitized** rather than deleted (`dropna()`):
- **Casualty & Fatality Distortion**: If rows with missing `Weather_Conditions` or `Road_Type` were dropped, over **7,900 crash records**—including **112 fatal accidents**—would be purged. This would artificially deflate national casualty tallies and bias risk models toward well-documented urban incidents.
- **Preservation of Statistical Integrity**: By recasting missing categories to `'Unknown'`, RRSIS retains every record for gross casualty and temporal analysis while isolating incomplete rows from narrow multi-factor cross-tabulations.

---

### SECTION 4: TASK 3 - TEMPORAL ACCIDENT INTELLIGENCE & DYNAMICS

#### 4.1 Chronological Breakdown
Task 3 evaluated crash distributions across temporal dimensions:

```
+---------------------------------------------------------------------------------------------------+
| TASK 3.1: DAY OF WEEK ACCIDENT DISTRIBUTION (N = 307,972 CLEAN RECORDS)                           |
+-------------------+-----------------------------+-------------------------------+-----------------+
| Day of Week       | Accident Count              | Percentage Share (%)          | Relative Rank   |
+-------------------+-----------------------------+-------------------------------+-----------------+
| Friday            | 50,529                      | 16.41%                        | 1 (Peak)        |
| Tuesday           | 46,385                      | 15.06%                        | 2               |
| Wednesday         | 46,381                      | 15.06%                        | 3               |
| Thursday          | 45,649                      | 14.82%                        | 4               |
| Monday            | 43,918                      | 14.26%                        | 5               |
| Saturday          | 41,566                      | 13.50%                        | 6               |
| Sunday            | 33,544                      | 10.89%                        | 7               |
+-------------------+-----------------------------+-------------------------------+-----------------+
```

- **Weekday vs. Weekend**: Weekdays generate **232,862 accidents (75.61%)**, whereas Weekends generate **75,110 accidents (24.39%)**.
- **Monthly Seasonality**: November leads with **29,095 crashes (9.45%)**, followed by October (**28,368 / 9.21%**) and July (**26,953 / 8.75%**), corresponding to seasonal rain and holiday traffic volumes.

#### 4.2 Custom Time Period Windows
Using PySpark `when/otherwise` logic, crashes were grouped into 5 custom operational time periods:

```
+---------------------------------------------------------------------------------------------------+
| TASK 3.2: OPERATIONAL TIME PERIOD DISTRIBUTION                                                    |
+---------------------+-------------------+-----------------+-------------------+-------------------+
| Time Period Window  | Hours Covered     | Crash Volume    | Percentage Share  | Avg Severity Wt   |
+---------------------+-------------------+-----------------+-------------------+-------------------+
| Afternoon           | 12:00 - 16:59     | 105,433         | 34.23%            | 1.29              |
| Morning             | 05:00 - 11:59     | 88,369          | 28.69%            | 1.29              |
| Evening             | 17:00 - 20:59     | 75,280          | 24.44%            | 1.32              |
| Night               | 21:00 - 23:59     | 23,989          | 7.79%             | 1.39              |
| Late Night          | 00:00 - 04:59     | 14,884          | 4.83%             | 1.52 (Highest!)   |
| Unknown             | Missing           | 17              | 0.01%             | 1.24              |
+---------------------+-------------------+-----------------+-------------------+-------------------+
```

#### 4.3 Identification of the Five Highest-Risk Time Periods with Numerical Proof
1. **Afternoon Commute Window (12:00–16:59)**: Highest total volume (**105,433 crashes, 34.23%**; Severity Score: 136,371).
2. **Morning Commute Window (05:00–11:59)**: Second highest volume (**88,369 crashes, 28.69%**; Severity Score: 113,771).
3. **Evening Transition Window (17:00–20:59)**: Highest early night volume (**75,280 crashes, 24.44%**; Severity Score: 99,116).
4. **Night Window (21:00–23:59)**: High-severity night window (**23,989 crashes, 7.79%**; Severity Score: 33,413; Avg Weight: 1.39).
5. **Late Night Window (00:00–04:59)**: Extreme trauma intensity (**14,884 crashes, 4.83%**; Severity Score: 22,572; **Highest Avg Severity Weight = 1.52**).

---

### SECTION 5: TASK 4 - ACCIDENT SEVERITY INDEX & RISK DIVERGENCE

#### 5.1 Mathematical Severity Formulation
Collisions were assigned weighted values:
$$	ext{Severity Weight} = egin{cases} 1, & 	ext{if Slight} \ 3, & 	ext{if Serious} \ 5, & 	ext{if Fatal} \end{cases}$$
$$	ext{Severity Score} = \sum (	ext{Accident Count} 	imes 	ext{Severity Weight})$$

#### 5.2 Multi-Dimensional Severity Aggregations

##### A. Location Severity Breakdown (Top 10 Districts)
```
+---------------------------------------------------------------------------------------------------+
| TASK 4.1: TOP 10 LOCATIONS BY TOTAL SEVERITY SCORE                                                |
+----+--------------------------+-------------------+---------------------+-------------------------+
| #  | Local Authority District | Accident Count    | Total Severity Score| Avg Severity Weight     |
+----+--------------------------+-------------------+---------------------+-------------------------+
| 1  | Birmingham               | 6,165             | 7,805               | 1.27                    |
| 2  | Leeds                    | 4,140             | 5,354               | 1.29                    |
| 3  | Westminster              | 2,811             | 4,341               | 1.54 (Highest Urban!)   |
| 4  | Manchester               | 3,132             | 3,854               | 1.23                    |
| 5  | Bradford                 | 3,006             | 3,840               | 1.28                    |
| 6  | Sheffield                | 2,750             | 3,464               | 1.26                    |
| 7  | Liverpool                | 2,611             | 3,445               | 1.32                    |
| 8  | Cornwall                 | 2,606             | 3,288               | 1.26                    |
| 9  | Cheshire East            | 2,125             | 2,995               | 1.41                    |
| 10 | County Durham            | 2,228             | 2,904               | 1.30                    |
+----+--------------------------+-------------------+---------------------+-------------------------+
```

##### B. Road Type Severity Breakdown
- **Single Carriageway**: **230,611 crashes** (74.88%), Total Severity Score: **307,427**, Avg Weight: **1.33**.
- **Dual Carriageway**: **45,467 crashes** (14.76%), Total Severity Score: **59,417**, Avg Weight: **1.31**.
- **Roundabout**: **20,929 crashes** (6.80%), Total Severity Score: **24,801**, Avg Weight: **1.19**.
- **One Way Street**: **6,197 crashes** (2.01%), Total Severity Score: **7,889**, Avg Weight: **1.27**.
- **Slip Road**: **3,234 crashes** (1.05%), Total Severity Score: **3,840**, Avg Weight: **1.19**.

##### C. Time Period Severity Breakdown
- **Afternoon**: 105,433 crashes, 136,371 score, 1.29 avg weight.
- **Morning**: 88,369 crashes, 113,771 score, 1.29 avg weight.
- **Evening**: 75,280 crashes, 99,116 score, 1.32 avg weight.
- **Night**: 23,989 crashes, 33,413 score, 1.39 avg weight.
- **Late Night**: 14,884 crashes, 22,572 score, **1.52 avg weight**.

##### D. Vehicle Type Severity Breakdown (Top 5)
- **Car**: 239,793 crashes, 315,487 score, 1.32 avg weight.
- **Van / Goods <=3.5t**: 15,695 crashes, 20,777 score, 1.32 avg weight.
- **Heavy Motorcycle >500cc**: 11,226 crashes, 14,790 score, 1.32 avg weight.
- **Bus or Coach**: 8,686 crashes, 11,300 score, 1.30 avg weight.
- **Light Motorcycle <=125cc**: 6,852 crashes, 9,028 score, 1.32 avg weight.

#### 5.3 Empirical Divergence Analysis
*Why the location with the most accidents is not necessarily the location with greatest safety risk*:
Comparing **Birmingham** vs. **Westminster**:
- Birmingham has **6,165 crashes** vs Westminster's **2,811 crashes** (more than 2.19x volume).
- However, Westminster records an **Average Severity Weight of 1.54** compared to Birmingham's **1.27**.
- Westminster has a significantly higher proportion of serious and fatal collisions per incident due to high pedestrian-vehicular conflict density. Ranking by raw volume alone would under-allocate safety resources to high-fatality locations.

---

### SECTION 6: TASK 5 - DANGEROUS-FACTOR COMBINATION ANALYSIS

Evaluating multi-attribute combinations (`Road_Type`, `Speed_limit`, `Weather_Conditions`, `Light_Conditions`, `Time_Period`):

```
+-----------------------------------------------------------------------------------------------------------------------------------+
| TASK 5: TOP 10 DANGEROUS FACTOR COMBINATIONS (RANKED BY TOTAL SEVERITY SCORE)                                                     |
+----+-------------------+-------------+--------------------+-----------------------+-------------+---------------+-----------------+
| #  | Road Type         | Speed Limit | Weather Condition  | Light Condition       | Time Period | Crash Volume  | Total Severity  |
+----+-------------------+-------------+--------------------+-----------------------+-------------+---------------+-----------------+
| 1  | Single Carriageway| 30 mph      | Fine No High Winds | Daylight              | Afternoon   | 47,707        | 60,443          |
| 2  | Single Carriageway| 30 mph      | Fine No High Winds | Daylight              | Morning     | 35,232        | 44,300          |
| 3  | Single Carriageway| 30 mph      | Fine No High Winds | Daylight              | Evening     | 21,083        | 27,089          |
| 4  | Single Carriageway| 60 mph      | Fine No High Winds | Daylight              | Afternoon   | 10,007        | 15,657 (1.56 Wt)|
| 5  | Single Carriageway| 30 mph      | Fine No High Winds | Darkness-Lights Lit   | Evening     | 11,512        | 15,136          |
| 6  | Single Carriageway| 60 mph      | Fine No High Winds | Daylight              | Morning     | 9,076         | 13,282 (1.46 Wt)|
| 7  | Single Carriageway| 30 mph      | Fine No High Winds | Darkness-Lights Lit   | Night       | 8,524         | 11,690          |
| 8  | Single Carriageway| 30 mph      | Fine No High Winds | Darkness-Lights Lit   | Late Night  | 5,185         | 7,709 (1.49 Wt) |
| 9  | Single Carriageway| 60 mph      | Fine No High Winds | Daylight              | Evening     | 4,297         | 6,927 (1.61 Wt) |
| 10 | Dual Carriageway  | 70 mph      | Fine No High Winds | Daylight              | Afternoon   | 4,560         | 6,012           |
+----+-------------------+-------------+--------------------+-----------------------+-------------+---------------+-----------------+
```

---

### SECTION 7: TASK 6 - ADVANCED WINDOW-BASED LOCATION RANKING

#### 7.1 Window Function Formulation
Using PySpark SQL Window API partitioned by `Urban_or_Rural_Area` ordered by `Severity_Score` descending:

```python
from pyspark.sql.window import Window

w_spec = Window.partitionBy("Urban_or_Rural_Area").orderBy(F.desc("Severity_Score"))

top3_window = loc_agg     .withColumn("Row_Num", F.row_number().over(w_spec))     .withColumn("Rank", F.rank().over(w_spec))     .withColumn("Dense_Rank", F.dense_rank().over(w_spec))     .filter(F.col("Row_Num") <= 3)
```

#### 7.2 Top 3 Ranking Output by Geographical Division

```
+-----------------------------------------------------------------------------------------------------------------------------------+
| TASK 6: TOP 3 RANKED LOCATIONS WITHIN URBAN VS RURAL GEOGRAPHICAL CATEGORIES                                                       |
+-------+--------------------+----------------+----------------+---------+------+------------+--------------------------------------+
| Area  | District Location  | Crash Volume   | Severity Score | Row_Num | Rank | Dense_Rank | Analytical Note                      |
+-------+--------------------+----------------+----------------+---------+------+------------+--------------------------------------+
| Rural | Cornwall           | 1,992          | 2,584          | 1       | 1    | 1          | Top Rural Hotspot (High single-lane) |
| Rural | County Durham      | 1,421          | 1,903          | 2       | 2    | 2          | 2nd Rural Hotspot (High speed road)  |
| Rural | Wiltshire          | 1,158          | 1,816          | 3       | 3    | 3          | 3rd Rural Hotspot (High night crash) |
+-------+--------------------+----------------+----------------+---------+------+------------+--------------------------------------+
| Urban | Birmingham         | 6,099          | 7,717          | 1       | 1    | 1          | Top Urban Hotspot (Metropolitan)     |
| Urban | Westminster        | 2,811          | 4,341          | 2       | 2    | 2          | 2nd Urban Hotspot (High avg weight)  |
| Urban | Leeds              | 3,329          | 4,231          | 3       | 3    | 3          | 3rd Urban Hotspot (Arterial junction)|
+-------+--------------------+----------------+----------------+---------+------+------------+--------------------------------------+
```

---

### SECTION 8: TASK 7 - COMPOSITE ROAD SAFETY RISK SCORE MODEL

#### 8.1 Mathematical Model Formulation & Component Normalization
To derive a balanced data-driven risk score, RRSIS combines 3 distinct dimensions:
1. **Severity Score ($S$)**: Weighted trauma burden (Weight = 40%).
2. **Accident Frequency ($F$)**: Operational exposure volume (Weight = 35%).
3. **Adverse Conditions Share ($A$)**: Proportion of crashes under Night/Late Night or Wet/Ice conditions (Weight = 25%).

**Min-Max Normalization Scaling**:
$$N_S = rac{S - S_{\min}}{S_{\max} - S_{\min}}, \quad N_F = rac{F - F_{\min}}{F_{\max} - F_{\min}}, \quad N_A = rac{A - A_{\min}}{A_{\max} - A_{\min}}$$

**Composite Risk Score Equation**:
$$	ext{Composite Risk Score} = (0.40 \cdot N_S + 0.35 \cdot N_F + 0.25 \cdot N_A) 	imes 100$$

#### 8.2 Top 10 High-Risk Locations Ranked by Composite Model

```
+-----------------------------------------------------------------------------------------------------------------------------------+
| TASK 7: COMPOSITE ROAD SAFETY RISK SCORE RANKING (SCALE 0 - 100)                                                                  |
+----+--------------------------+---------------+----------------+----------------+---------------------+---------------------------+
| #  | District Location        | Crash Volume  | Severity Score | Adverse Share  | Composite Risk Score| Risk Category             |
+----+--------------------------+---------------+----------------+----------------+---------------------+---------------------------+
| 1  | Birmingham               | 6,165         | 7,805          | 14.29%         | 88.54               | CRITICAL HOTSPOT (Priority 1)
| 2  | Leeds                    | 4,140         | 5,354          | 13.21%         | 63.40               | HIGH HOTSPOT (Priority 2) |
| 3  | Westminster              | 2,811         | 4,341          | 16.93%         | 54.16               | HIGH HOTSPOT (Priority 3) |
| 4  | Manchester               | 3,132         | 3,854          | 15.20%         | 51.84               | HIGH HOTSPOT (Priority 4) |
| 5  | Bradford                 | 3,006         | 3,840          | 12.97%         | 48.95               | ELEVATED HOTSPOT (Pri. 5) |
| 6  | Liverpool                | 2,611         | 3,445          | 15.43%         | 47.00               | ELEVATED HOTSPOT          |
| 7  | Sheffield                | 2,750         | 3,464          | 12.62%         | 45.22               | ELEVATED HOTSPOT          |
| 8  | Lambeth                  | 2,250         | 2,884          | 15.91%         | 42.52               | MODERATE HOTSPOT          |
| 9  | Cornwall                 | 2,606         | 3,288          | 11.55%         | 42.49               | MODERATE HOTSPOT          |
| 10 | Bristol, City Of         | 2,270         | 2,760          | 14.63%         | 40.78               | MODERATE HOTSPOT          |
+----+--------------------------+---------------+----------------+----------------+---------------------+---------------------------+
```

---

### SECTION 9: TASK 8 - SPARK EXECUTION, ARCHITECTURE & PERFORMANCE ANALYSIS

#### 9.1 Catalyst Optimizer & Physical Plan Output (`df.explain(True)`)
PySpark translates DataFrame queries through 4 Catalyst optimization phases:
1. **Parsed Logical Plan**: Unresolved relation tree.
2. **Analyzed Logical Plan**: Schema resolution via Catalog.
3. **Optimized Logical Plan**: Filter pushdown, projection pruning, boolean simplification.
4. **Physical Plan**: Executable operator tree specifying scan operators, joins, and shuffles (`Exchange`).

#### 9.2 Transformations vs. Actions, Jobs, Stages, and Tasks
- **Transformations**: `filter()`, `withColumn()`, `groupBy()`, `select()` are **lazy**; they construct the DAG without execution.
- **Actions**: `count()`, `show()`, `first()`, `collect()` trigger execution.
- **Jobs, Stages, Tasks**: Each action starts 1 Job. Jobs split into Stages at wide shuffle boundaries (`Exchange`). Each stage creates Tasks (1 per partition).

#### 9.3 Wide Dependencies & Network Shuffle Mechanics (`Exchange hashpartitioning`)
Operations such as `groupBy("Local_Authority_District")`, `dropDuplicates()`, and `Window` functions cause **wide dependencies**:
- Data records with the same group key reside on different cluster executors.
- Spark executes an `Exchange hashpartitioning` operator: Map tasks serialize rows by hash key to local disk, and Reduce tasks fetch key partitions over the cluster network.

#### 9.4 Memory Optimization & `cache()` Evaluation
We call `df_clean.cache()` immediately after Task 2 sanitization:
- **Branching DAG Workload**: Tasks 3, 4, 5, 6, 7, and 9 all branch from `df_clean`.
- Without `cache()`, Spark would re-evaluate the raw HDFS read and cleaning pipeline 6 separate times.
- With `cache()`, `df_clean` is stored in executor memory (`MEMORY_AND_DISK`), reducing total execution time from over 45 seconds down to ~4 seconds across downstream tasks.

---

### SECTION 10: TASK 9 - FINAL MANAGEMENT CHALLENGE (5 PRIORITY POLICY INTERVENTIONS)

Road safety authorities can prioritize only **five intervention areas**. Based entirely on our Spark analysis, the 5 priorities are:

```
+-----------------------------------------------------------------------------------------------------------------------------------+
| TASK 9: MANAGEMENT CHALLENGE - TOP 5 PRIORITIZED ROAD SAFETY INTERVENTIONS                                                       |
+---+-----------------------------------+---------------------------------------------------------+---------------------------------+
| # | Intervention Focus Area           | Spark Empirical Evidence                                | Strategic Policy Action         |
+---+-----------------------------------+---------------------------------------------------------+---------------------------------+
| 1 | High-Speed Single Carriageway     | 230,611 crashes (74.9%), 307,427 severity score (76.0%);| Install median barriers, rumble |
|   | Infrastructure Upgrades           | 60 mph single carriageways have highest fatality rate.  | strips, speed calming on RN1/3. |
+---+-----------------------------------+---------------------------------------------------------+---------------------------------+
| 2 | Nocturnal & Evening Traffic       | Evening (17-20: 24.4%) & Night/Late Night (21-04: 12.6%)| Deploy mobile radar checkpoints |
|   | Police Enforcement (17:00-02:00)  | account for 37%+ crashes; Late Night avg weight = 1.52. | & breathalyzer patrols at night.|
+---+-----------------------------------+---------------------------------------------------------+---------------------------------+
| 3 | Resource Deployment in Top 3      | Window ranking & Risk Score identify Birmingham (88.54),| Concentrate 60% of enforcement  |
|   | Ranked High-Risk Hotspots         | Leeds (63.40), Westminster (54.16) & Cornwall (Rural #1)| and emergency stations in top 3.|
+---+-----------------------------------+---------------------------------------------------------+---------------------------------+
| 4 | High-Friction Resurfacing &       | Adverse weather/wet surfaces present in 15%+ crashes,   | Mandate high-friction asphalt   |
|   | Roadside Drainage Upgrades        | multiplying braking distance on steep downhill curves.  | & clear drainage before rains.  |
+---+-----------------------------------+---------------------------------------------------------+---------------------------------+
| 5 | Commercial Fleet & Bus Speed      | Heavy goods vehicles (>7.5t) & buses cause 15,218+ high-| Enforce mandatory GPS speed     |
|   | Governor Audits & Patrols         | severity crashes with avg weight > 1.31 due to momentum.| governors & driver rest hours.  |
+---+-----------------------------------+---------------------------------------------------------+---------------------------------+
```

---

### SECTION 11: RWANDAN IMPLEMENTATION FRAMEWORK, DATA AVAILABILITY & POLICY ROADMAP

#### 11.1 Surrogate Dataset Transparency vs Official NISR Micro-Data Integration
As documented in the case study:
- **Current Development Phase**: The Kaggle Road Accident Dataset (307,973 records) served as a surrogate dataset to engineer, test, and validate RRSIS algorithms and Spark execution plans.
- **Rwanda NISR Benchmark**: Official NISR Statistical Yearbook 2024 records **9,995 total accidents in 2023** and **761 fatal crashes** in Rwanda.

#### 11.2 Rwanda National Police (RNP) Digitization Timeline & Data Availability
1. **Current Status**: RNP records accident logs in digitized police station registration databases.
2. **Integration Availability**: NISR and RNP plan to release anonymized micro-data crash tables via the NISR Open Data Portal. RRSIS is designed to ingest these CSV/Parquet micro-data files directly into HDFS without modifying the Spark pipeline logic.

#### 11.3 Policy Recommendations for Rwanda Road Safety Authorities
1. **Corridor Enforcement**: Deploy automated speed cameras along National Roads **RN1 (Kigali–Huye–Akanyaru)** and **RN3 (Kigali–Musanze–Rubavu)**.
2. **Gerayo Amahoro Campaign Alignment**: Focus public sensitization on evening/nocturnal speeding and pedestrian visibility during wet rain seasons.

---

### SECTION 12: CONCLUSION & REFERENCES

RRSIS demonstrates that a big data architecture combining **HDFS** and **Apache Spark** provides the computational performance and scalability required to process high-volume road safety data, calculate multi-dimensional risk scores, and deliver actionable management decisions for road safety authorities.

#### Key References:
1. National Institute of Statistics of Rwanda (NISR), *Statistical Yearbook 2024*, Chapter 14: Transport and Communication, Table 14.2.6: Road Accidents.
2. Rwanda National Police (RNP), *Annual Road Safety Enforcement and Crash Statistics Report 2023*.
3. Apache Spark Documentation, *Spark SQL, DataFrames and Datasets Guide*, Apache Software Foundation.
4. Zaharia, M., et al., *Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing*, NSDI 2012.
