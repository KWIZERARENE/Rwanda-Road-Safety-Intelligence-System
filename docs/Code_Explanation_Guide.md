# RWANDA ROAD SAFETY INTELLIGENCE SYSTEM (RRSIS)
## CODE EXPLANATION GUIDE & VIVA-VOCE EXAMINATION PREPARATION

---

### OVERVIEW & PURPOSE
This guide provides a comprehensive, cell-by-cell and line-by-line explanation of the PySpark source code implemented in `notebooks/RRSIS_Full_Analysis.ipynb`. It is specifically designed to prepare group members for the **Viva-Voce Oral Defense (Task 10)**.

---

### TABLE OF CONTENTS
1. **Task 1: HDFS Ingestion & Spark Partitioning Explanation**
2. **Task 2: Data Quality Engineering & BEFORE-Cleaning Audit**
3. **Task 3: Temporal Accident Intelligence Code Breakdown**
4. **Task 4: Accident Severity Index & Multi-Dimensional Code Breakdown**
5. **Task 5: Dangerous Factor Combination Analysis Code Breakdown**
6. **Task 6: PySpark Window Functions Code Breakdown**
7. **Task 7: Data-Driven Composite Risk Score Model Code Breakdown**
8. **Task 8: Spark Execution Plan & Catalyst Optimizer Explanation (`df.explain(True)`)**
9. **Task 9: Management Priorities Strategy Matrix**
10. **Task 10: Geospatial Mapping & Visualization Mechanics**
11. **Viva-Voce Question & Answer Knowledge Base (Top 20 Questions)**

---

### SECTION 1: TASK 1 - HDFS DATA INGESTION & PARTITION DIAGNOSTICS

```python
# 1. Initialize PySpark Session
spark = SparkSession.builder     .appName("Rwanda_Road_Safety_Intelligence_System")     .config("spark.driver.memory", "4g")     .config("spark.sql.shuffle.partitions", "8")     .getOrCreate()
```
- `SparkSession.builder`: The entry point to programming Spark with the DataFrame API.
- `.config("spark.sql.shuffle.partitions", "8")`: Sets default partitions for wide operations to 8 (matching cluster core availability).

```python
# 2. Read Dataset from HDFS (with local fallback)
df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(HDFS_DATASET_PATH)
```
- `option("header", "true")`: Uses the first line of CSV as column names.
- `option("inferSchema", "true")`: Automatically samples records to infer double, integer, timestamp, and string column types.

```python
# 3. Partition Tracking
df_partitioned = df_raw.withColumn("partition_id", F.spark_partition_id())
df_partitioned.groupBy("partition_id").count().orderBy("partition_id").show()
```
- `F.spark_partition_id()`: Returns the internal partition index for each record, allowing verification of even load balancing across executor cores.

---

### SECTION 2: TASK 2 - DATA QUALITY ENGINEERING & BEFORE-CLEANING AUDIT

```python
# 1. Audit Missing / Null Counts BEFORE Cleaning
null_audit_exprs = [
    F.count(
        F.when(
            F.col(c).isNull() | 
            (F.col(c).cast("string") == "") | 
            (F.col(c).cast("string") == "None") | 
            (F.col(c).cast("string") == "NULL"), 
            c
        )
    ).alias(c) for c in df_raw.columns
]
df_null_audit = df_raw.select(null_audit_exprs)
```
- **Line Explanation**: Uses list comprehension over all columns to evaluate nulls, empty strings, and string sentinels before applying cleaning functions.

```python
# 2. Cleaning Operations
df_clean = df_raw
for c in cat_cols:
    df_clean = df_clean.withColumn(c, F.initcap(F.trim(F.col(c).cast("string"))))

df_clean = df_clean.withColumn("Accident_Severity", F.when(F.col("Accident_Severity") == "Fetal", "Fatal").otherwise(F.col("Accident_Severity")))
df_clean = df_clean.dropDuplicates()

for c in cat_cols:
    df_clean = df_clean.withColumn(c, F.when(F.col(c).isNull() | (F.trim(F.col(c)) == "") | (F.col(c) == "None"), "Unknown").otherwise(F.col(c)))

df_clean.cache()
```
- `F.initcap(F.trim(...))`: Converts string values to title case and strips leading/trailing spaces.
- `F.when(... == "Fetal", "Fatal")`: Fixes typo without dropping records.
- `dropDuplicates()`: Performs wide shuffle to eliminate duplicate row structs.
- `.cache()`: Persists sanitized dataset in executor memory (`MEMORY_AND_DISK`) to prevent redundant HDFS reads in downstream tasks.

---

### SECTION 3: TASK 3 - TEMPORAL ACCIDENT INTELLIGENCE

```python
# 1. Extract Hour of Day
extracted_hour = F.coalesce(
    F.hour(F.to_timestamp(F.col("Time"))),
    F.regexp_extract(F.col("Time"), r"(\d{1,2}):", 1).cast("integer")
)

# 2. Categorize Time Periods
df_time = df_clean.withColumn("Hour_of_Day", extracted_hour)     .withColumn("Time_Period",
        F.when(F.col("Hour_of_Day").isNull(), "Unknown")
         .when((F.col("Hour_of_Day") >= 0) & (F.col("Hour_of_Day") <= 4), "Late Night")
         .when((F.col("Hour_of_Day") >= 5) & (F.col("Hour_of_Day") <= 11), "Morning")
         .when((F.col("Hour_of_Day") >= 12) & (F.col("Hour_of_Day") <= 16), "Afternoon")
         .when((F.col("Hour_of_Day") >= 17) & (F.col("Hour_of_Day") <= 20), "Evening")
         .when((F.col("Hour_of_Day") >= 21) & (F.col("Hour_of_Day") <= 23), "Night")
         .otherwise("Unknown")
    )
```
- `F.coalesce(...)`: Tries parsing time as timestamp first; if null, falls back to regex extraction (`regexp_extract`).
- `F.when(... & ...)`: Evaluates multi-condition range bounds to assign 5 operational time periods.

---

### SECTION 4: TASK 4 - ACCIDENT SEVERITY INDEX & MULTI-DIMENSIONAL BREAKDOWN

```python
# 1. Severity Weight Mapping
df_sev = df_time.withColumn(
    "Severity_Weight",
    F.when(F.col("Accident_Severity") == "Slight", 1)
     .when(F.col("Accident_Severity") == "Serious", 3)
     .when(F.col("Accident_Severity") == "Fatal", 5)
     .otherwise(1)
)

# 2. Multi-Dimensional Aggregation
df_sev.groupBy("Local_Authority_District").agg(
    F.count("*").alias("Accident_Count"),
    F.sum("Severity_Weight").alias("Severity_Score"),
    F.round(F.avg("Severity_Weight"), 2).alias("Avg_Severity_Weight")
).orderBy(F.desc("Severity_Score")).show(10)
```
- Applies severity weights ($	ext{Slight}=1, 	ext{Serious}=3, 	ext{Fatal}=5$).
- Aggregates by `Local_Authority_District`, `Road_Type`, `Vehicle_Type`, and `Time_Period`.

---

### SECTION 5: TASK 5 - DANGEROUS FACTOR COMBINATION ANALYSIS

```python
factor_cols = ["Road_Type", "Speed_limit", "Weather_Conditions", "Light_Conditions", "Time_Period"]

top10_combinations = df_sev.groupBy(*factor_cols).agg(
    F.count("*").alias("Accident_Count"),
    F.sum("Severity_Weight").alias("Total_Severity_Score"),
    F.round(F.avg("Severity_Weight"), 2).alias("Avg_Severity_Per_Accident")
).orderBy(F.desc("Total_Severity_Score")).limit(10)
```
- Multi-attribute grouping over 5 dimension columns to isolate compound risk tuples.

---

### SECTION 6: TASK 6 - ADVANCED LOCATION RANKING VIA WINDOW FUNCTIONS

```python
from pyspark.sql.window import Window

# 1. Define Window Specification
window_spec = Window.partitionBy("Urban_or_Rural_Area").orderBy(F.desc("Severity_Score"))

# 2. Apply Window Ranking Functions
top3_ranked = loc_agg     .withColumn("Row_Num", F.row_number().over(window_spec))     .withColumn("Rank", F.rank().over(window_spec))     .withColumn("Dense_Rank", F.dense_rank().over(window_spec))     .filter(F.col("Row_Num") <= 3)
```
- `Window.partitionBy("Urban_or_Rural_Area")`: Groups data independently for Urban vs Rural categories.
- `F.row_number().over(window_spec)`: Assigns sequential integers without ties.
- `F.rank()` vs `F.dense_rank()`: Demonstrates handling of tied scores (rank leaves gaps, dense rank does not).

---

### SECTION 7: TASK 7 - COMPOSITE ROAD SAFETY RISK SCORE MODEL

```python
# 1. Min-Max Normalization Components
norm_sev = (F.col("Severity_Score") - stats["min_s"]) / (stats["max_s"] - stats["min_s"])
norm_freq = (F.col("Frequency") - stats["min_f"]) / (stats["max_f"] - stats["min_f"])
norm_adv = (F.col("Adverse_Share") - stats["min_a"]) / (stats["max_a"] - stats["min_a"])

# 2. Weighted Score (0 to 100)
risk_scored = loc_risk.withColumn(
    "Composite_Risk_Score",
    F.round((0.40 * norm_sev + 0.35 * norm_freq + 0.25 * norm_adv) * 100, 2)
).orderBy(F.desc("Composite_Risk_Score"))
```
- Min-Max scales all components between 0 and 1 before applying weights (40% Severity, 35% Frequency, 25% Adverse Share).

---

### SECTION 8: TASK 8 - SPARK EXECUTION & PERFORMANCE ANALYSIS (`df.explain(True)`)

```python
risk_scored_locations.explain(True)
```
- Dumps Catalyst Optimizer plan stages: Parsed Logical Plan, Analyzed Logical Plan, Optimized Logical Plan, Physical Plan.
- Explains wide dependencies (`Exchange hashpartitioning`) and network shuffle bottlenecks.

---

### SECTION 9: TASK 9 - MANAGEMENT CHALLENGE STRATEGY

Framed strictly as:
$$	ext{Data} \longrightarrow 	ext{Spark Analysis} \longrightarrow 	ext{Evidence} \longrightarrow 	ext{Recommendation}$$
Presents top 5 priorities with exact empirical numbers.

---

### SECTION 10: VIVA-VOCE EXAMINATION KNOWLEDGE BASE (TOP 20 QUESTIONS)

1. **Q: Why use PySpark DataFrames instead of RDDs or Pandas?**  
   *A*: PySpark DataFrames leverage the Catalyst Optimizer and Tungsten execution engine for off-heap memory management and query optimization. Unlike Pandas, DataFrames execute out-of-core across distributed cluster nodes.

2. **Q: What is a lazy transformation in Spark? Give examples.**  
   *A*: Lazy transformations (`filter`, `select`, `withColumn`, `groupBy`) do not execute immediately; Spark builds a Directed Acyclic Graph (DAG). Execution only occurs when an action (`count`, `show`, `collect`) is called.

3. **Q: What causes a network shuffle in Spark?**  
   *A*: Wide transformations (`groupBy`, `dropDuplicates`, `join`, `Window`) require re-partitioning data across nodes by key, triggering `Exchange hashpartitioning` which involves disk serialization and network transfer.

4. **Q: Why call `cache()` in Task 2?**  
   *A*: `df_clean` is consumed by multiple downstream tasks (Tasks 3 through 9). Calling `cache()` stores the sanitized DataFrame in executor memory (`MEMORY_AND_DISK`), preventing Spark from re-reading HDFS 6 separate times.

5. **Q: Why not delete rows with missing values (`dropna()`)?**  
   *A*: Naive deletion would drop over 7,900 crashes including 112 fatal accidents, creating severe selection bias. Imputing `"Unknown"` preserves total casualty counts.

6. **Q: What is the difference between `rank()` and `dense_rank()` in Window functions?**  
   *A*: `rank()` leaves gaps after tied values (1, 2, 2, 4), whereas `dense_rank()` assigns consecutive ranks (1, 2, 2, 3).

7. **Q: How does Min-Max normalization work in Task 7?**  
   *A*: Scales values between 0 and 1: $N_X = (X - X_{\min}) / (X_{\max} - X_{\min})$, enabling apples-to-apples weighting across metrics with different units.

8. **Q: How does RRSIS relate to Rwanda accident data?**  
   *A*: The Kaggle dataset serves as an enterprise surrogate for engineering the PySpark pipeline. Official NISR 2024 Yearbook statistics (9,995 crashes, 761 fatalities) provide national benchmarks for policy recommendations.
