# EXECUTIVE FINAL PRESENTATION
## RWANDA ROAD SAFETY INTELLIGENCE SYSTEM (RRSIS)
**HDFS + Apache Spark (PySpark DataFrame API) Analytics Engine**

---

## SLIDE 1: Title & Project Overview
- **System**: Rwanda Road Safety Intelligence System (RRSIS)
- **Engine**: Apache Spark 3.x (PySpark DataFrame API) + Hadoop HDFS Storage
- **Project Team**:
  - Amie Marie Flora Dushimumukiza (Reg ID: 101405) - Data Quality & Window Ranking
  - Kwizera Rene (Reg ID: 101379) - HDFS Ingestion, Catalyst Execution & Performance Analysis
  - Grace Teta (Reg ID: 101378) - Temporal Intelligence, Severity Modeling & Management Strategy

---

## SLIDE 2: Case Study Context & Dataset Transparency
- **Rwanda NISR Official Benchmark (2024)**:
  - Table 14.2.6 (Road Accidents): 9,995 total crashes in Rwanda in 2023, including 761 fatal accidents.
  - Surge: Total crashes increased by +137.8% between 2020 (4,203) and 2023 (9,995).
- **Enterprise Surrogate Dataset Disclosure**:
  - Kaggle Road Accident Dataset (307,973 records, 23 attributes) used as surrogate dataset for prototype building.
  - Transparently disclosed: Kaggle rows are surrogate data for testing PySpark; NISR data provides national benchmarks.

---

## SLIDE 3: Task 1 - HDFS Storage & Data Ingestion Architecture
- **HDFS Target URI**: `hdfs://localhost:9000/user/hadoop/rrsis/raw/road_accidents_23cols.csv`
- **Total Ingested Dataset**: 307,973 records across 23 columns.
- **Partition Diagnostics**: 8 RDD partitions audited via `F.spark_partition_id()`.

---

## SLIDE 4: Task 2 - Data Quality Audit BEFORE Cleaning & Sanitization
- **Missing Value Audit BEFORE Cleaning**:
  - `Carriageway_Hazards`: 302,549 nulls (98.24%).
  - `Weather_Conditions`: 6,057 nulls (1.97%).
  - `Road_Type`: 1,534 nulls (0.50%).
  - `Road_Surface_Conditions`: 317 nulls (0.10%).
  - `Time`: 17 nulls (0.01%).
  - Typo: `'Fetal'` entries under `Accident_Severity`.
- **Sanitization Execution**: `initcap`, `trim`, typo fix `'Fetal'` -> `'Fatal'`, `dropDuplicates()`, `Unknown` imputation, and `cache()`.

---

## SLIDE 5: Task 3 - Temporal Accident Intelligence
- **Day of Week**: Friday peak (50,529 crashes / 16.41%). Weekdays (75.61%) vs Weekend (24.39%).
- **Monthly Seasonality**: November peak (29,095 / 9.45%), October (9.21%), July (8.75%).
- **5 Time Periods**: Afternoon (34.23%), Morning (28.69%), Evening (24.44%), Night (7.79%), Late Night (4.83%, highest severity weight = 1.52).

---

## SLIDE 6: Task 4 - Multi-Dimensional Accident Severity Index
- **Weights**: Slight = 1, Serious = 3, Fatal = 5.
- **Road Type Burden**: Single Carriageway accounts for 230,611 crashes (74.9%) and 307,427 severity score (76.0%).
- **Frequency vs Severity Divergence**: Westminster (Avg Severity = 1.54) vs Birmingham (1.27) demonstrates why high crash volume does not equal highest safety risk per crash.

---

## SLIDE 7: Task 5 - Top 10 Dangerous Factor Combinations
- Top risk tuples isolate single carriageways at 30 mph and 60 mph speed limits during afternoon and late night periods.
- 60 mph single carriageways elevate average severity weight to 1.56-1.61 per crash.

---

## SLIDE 8: Task 6 - Window Function Location Rankings
- **PySpark Window Spec**: `Window.partitionBy('Urban_or_Rural_Area').orderBy(F.desc('Severity_Score'))`.
- **Top 3 Urban**: 1. Birmingham (7,717 score), 2. Westminster (4,341 score), 3. Leeds (4,231 score).
- **Top 3 Rural**: 1. Cornwall (2,584 score), 2. County Durham (1,903 score), 3. Wiltshire (1,816 score).

---

## SLIDE 9: Task 7 - Data-Driven Composite Road Safety Risk Score Model
- **Min-Max Formula**: $N_X = (X - X_{\min}) / (X_{\max} - X_{\min})$.
- **Composite Equation**: $(0.40 \cdot N_S + 0.35 \cdot N_F + 0.25 \cdot N_A) 	imes 100$.
- **Top Ranked Locations**: 1. Birmingham (88.54), 2. Leeds (63.40), 3. Westminster (54.16), 4. Manchester (51.84), 5. Bradford (48.95).

---

## SLIDE 10: Task 8 - Spark Execution & Distributed Performance Analysis
- **Physical Execution Plan**: `df.explain(True)` identifies logical/physical plans and wide dependencies (`Exchange hashpartitioning`).
- **Memory Optimization**: `cache()` on `df_clean` reduced execution time from >45s to ~4s across downstream tasks.

---

## SLIDE 11: Task 9 - Management Challenge: Top 5 Strategic Priorities
1. **High-Speed Single Carriageway Engineering**: Median physical barriers & speed calming on RN1/RN3 corridors.
2. **Nocturnal Traffic Enforcement Window (17:00-02:00)**: Mobile radar & breathalyzer checkpoints.
3. **Targeted Hotspot Resource Allocation**: Concentrate 60% of budget in top ranked locations.
4. **High-Friction Resurfacing & Drainage**: Anti-skid pavement overlays on downhill curves.
5. **Commercial Vehicle & Bus Speed Governors**: Mandatory GPS speed governor audits.

---

## SLIDE 12: Rwanda Implementation Framework & Policy Roadmap
- **Data Availability**: NISR & Rwanda National Police (RNP) crash micro-data release timeline.
- **Gerayo Amahoro Integration**: Automated ANPR cameras on national trunk roads (RN1, RN3).
