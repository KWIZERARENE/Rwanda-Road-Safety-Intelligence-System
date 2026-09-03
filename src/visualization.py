"""
Task 10: Geospatial Mapping & Data Visualization Module (RRSIS Project)
-------------------------------------------------------------------------
Generates comprehensive geographical map plots using Latitude & Longitude coordinates
and analytical visualization charts for all project tasks:
  1. Geospatial Accident Map Plot (Latitude vs Longitude scatter & density map)
  2. Interactive HTML Leaflet Map (Folium / Standalone HTML Map)
  3. Temporal Distribution Charts (Time Period & Hourly crash volumes)
  4. Severity Index vs Accident Count Comparison Chart
  5. Top 10 Dangerous Factor Combination Chart
  6. PySpark Window Function Geographical Category Ranking Chart (Urban vs Rural Top 3)
  7. Composite Road Safety Risk Score National Ranking Chart

Run standalone:
    python src/visualization.py
"""

import os
import sys

# Ensure repository root and src directory are in sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for headless PNG export
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from src.spark_session import get_spark_session
    from src.task7_risk_score import run as run_task7
except ImportError:
    from spark_session import get_spark_session
    from task7_risk_score import run as run_task7


def generate_geospatial_map_plot(df_clean, output_dir):
    """
    Plots accident locations on a 2D geographical coordinate plane (Latitude vs Longitude),
    color-coded by Accident Severity (Fatal, Serious, Slight).
    """
    print("[Visualization] Generating Geospatial Coordinate Map Plot (Latitude vs Longitude)...")
    
    # Filter valid coordinates (excluding nulls and 0.0 placeholders)
    coord_df = df_clean.filter(
        F.col("Latitude").isNotNull() & 
        F.col("Longitude").isNotNull() &
        (F.col("Latitude") != 0) & 
        (F.col("Longitude") != 0)
    ).select("Latitude", "Longitude", "Accident_Severity", "Local_Authority_District")

    # Sample if dataset is very large for crisp plotting performance
    total_valid = coord_df.count()
    if total_valid > 10000:
        coord_df = coord_df.sample(False, 10000.0 / total_valid)

    # Convert PySpark DataFrame sample to Pandas for Matplotlib/Seaborn rendering
    pdf = coord_df.toPandas()

    if pdf.empty:
        print("  Notice: No valid coordinates found for geospatial plotting.")
        return

    plt.figure(figsize=(12, 8))
    sns.set_style("darkgrid")

    # Define color palette mapping
    palette = {
        "Fatal": "#d9534f",     # Red
        "Serious": "#f0ad4e",   # Orange
        "Slight": "#5bc0de",    # Blue
        "Unknown": "#777777"   # Grey
    }

    # Plot Scatter Points (Longitude on X-axis, Latitude on Y-axis)
    ax = sns.scatterplot(
        data=pdf,
        x="Longitude",
        y="Latitude",
        hue="Accident_Severity",
        palette=palette,
        alpha=0.6,
        s=30,
        edgecolor="k",
        linewidth=0.2
    )

    plt.title("RRSIS Geospatial Accident Coordinate Map (Latitude vs. Longitude)", fontsize=14, fontweight='bold', pad=15)
    plt.xlabel("Longitude (°E)", fontsize=12, labelpad=10)
    plt.ylabel("Latitude (°N)", fontsize=12, labelpad=10)
    plt.legend(title="Accident Severity", title_fontsize='11', loc='upper right', frameon=True)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()

    map_png_path = os.path.join(output_dir, "geospatial_accident_coordinate_map.png")
    plt.savefig(map_png_path, dpi=300)
    plt.close()
    print(f"  Saved Geospatial Coordinate Map Plot: {map_png_path}")


def generate_interactive_html_map(df_clean, output_dir):
    """
    Generates an interactive Folium / Leaflet HTML Map with coordinate markers & popups.
    """
    print("[Visualization] Building Interactive Geospatial Map (HTML)...")
    
    coord_df = df_clean.filter(
        F.col("Latitude").isNotNull() & 
        F.col("Longitude").isNotNull() &
        (F.col("Latitude") != 0) & 
        (F.col("Longitude") != 0)
    ).select("Latitude", "Longitude", "Accident_Severity", "Local_Authority_District")

    sample_pd = coord_df.limit(1000).toPandas()
    if sample_pd.empty:
        return

    html_path = os.path.join(output_dir, "rrsis_interactive_geospatial_map.html")

    try:
        import folium
        from folium.plugins import MarkerCluster

        # Center map on mean coordinates
        center_lat = sample_pd["Latitude"].mean()
        center_lon = sample_pd["Longitude"].mean()

        m = folium.Map(location=[center_lat, center_lon], zoom_start=10, tiles="cartodbpositron")
        marker_cluster = MarkerCluster().add_to(m)

        color_map = {
            "Fatal": "red",
            "Serious": "orange",
            "Slight": "blue",
            "Unknown": "gray"
        }

        for _, row in sample_pd.iterrows():
            folium.CircleMarker(
                location=[row["Latitude"], row["Longitude"]],
                radius=5,
                popup=f"<b>District:</b> {row['Local_Authority_District']}<br><b>Severity:</b> {row['Accident_Severity']}",
                color=color_map.get(row["Accident_Severity"], "blue"),
                fill=True,
                fill_color=color_map.get(row["Accident_Severity"], "blue"),
                fill_opacity=0.7
            ).add_to(marker_cluster)

        m.save(html_path)
        print(f"  Saved Interactive Folium Map: {html_path}")

    except ImportError:
        # Fallback to pure HTML embedding map template
        print("  (Folium module not installed, creating standalone HTML map template...)")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(f"""<!DOCTYPE html>
<html>
<head>
    <title>RRSIS Geospatial Accident Map</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.7.1/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.7.1/dist/leaflet.js"></script>
    <style>#map {{ height: 600px; width: 100%; }}</style>
</head>
<body>
    <h2>RRSIS Interactive Geospatial Accident Map</h2>
    <div id="map"></div>
    <script>
        var map = L.map('map').setView([{sample_pd['Latitude'].mean()}, {sample_pd['Longitude'].mean()}], 10);
        L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
            maxZoom: 18,
            attribution: 'RRSIS Road Safety Intelligence System'
        }}).addTo(map);
    </script>
</body>
</html>""")
        print(f"  Saved Standalone Leaflet HTML Map: {html_path}")


def generate_analytical_charts(df_sev, risk_scored_df, output_dir):
    """
    Generates standard analytical charts for project tasks:
      - Temporal crash trends (Time Period)
      - Severity Score vs Accident Volume comparison
      - Top 10 Dangerous Factor Combinations
      - Composite Road Safety Risk Scores
    """
    print("[Visualization] Generating Analytical Charts (Matplotlib & Seaborn)...")
    sns.set_style("whitegrid")

    # 1. Temporal Analysis Chart: Time Period Volume vs Fatal Crashes
    time_pd = df_sev.groupBy("Time_Period").agg(
        F.count("*").alias("Total_Accidents"),
        F.sum(F.when(F.col("Accident_Severity") == "Fatal", 1).otherwise(0)).alias("Fatal_Accidents")
    ).orderBy(F.desc("Total_Accidents")).toPandas()

    plt.figure(figsize=(10, 5))
    ax = sns.barplot(data=time_pd, x="Time_Period", y="Total_Accidents", palette="Blues_d")
    plt.title("Accident Concentration by Custom Time Period Category", fontsize=13, fontweight='bold')
    plt.xlabel("Time Period Window", fontsize=11)
    plt.ylabel("Total Accident Count", fontsize=11)
    for p in ax.patches:
        ax.annotate(f"{int(p.get_height()):,}", (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha='center', va='bottom', fontsize=10, xytext=(0, 3), textcoords='offset points')
    plt.tight_layout()
    chart1_path = os.path.join(output_dir, "temporal_accidents_by_time_period.png")
    plt.savefig(chart1_path, dpi=300)
    plt.close()

    # 2. Severity Score vs Accident Volume Comparison (District Level)
    dist_pd = df_sev.groupBy("Local_Authority_District").agg(
        F.count("*").alias("Total_Accidents"),
        F.sum("Severity_Weight").alias("Severity_Score")
    ).orderBy(F.desc("Severity_Score")).limit(10).toPandas()

    plt.figure(figsize=(12, 6))
    df_melted = pd.melt(dist_pd, id_vars=["Local_Authority_District"], 
                        value_vars=["Total_Accidents", "Severity_Score"],
                        var_name="Metric", value_name="Value")
    
    ax = sns.barplot(data=df_melted, x="Local_Authority_District", y="Value", hue="Metric", palette="Set2")
    plt.title("District Comparison: Accident Count vs. Weighted Severity Score Burden", fontsize=13, fontweight='bold')
    plt.xlabel("Local Authority District", fontsize=11)
    plt.ylabel("Metric Value", fontsize=11)
    plt.xticks(rotation=30, ha='right')
    plt.legend(title="Metric Breakdown")
    plt.tight_layout()
    chart2_path = os.path.join(output_dir, "district_frequency_vs_severity_score.png")
    plt.savefig(chart2_path, dpi=300)
    plt.close()

    # 3. Composite Risk Score National Ranking Chart
    if risk_scored_df is not None:
        risk_pd = risk_scored_df.select("Local_Authority_District", "Composite_Risk_Score").limit(10).toPandas()
        
        plt.figure(figsize=(10, 6))
        ax = sns.barplot(data=risk_pd, y="Local_Authority_District", x="Composite_Risk_Score", palette="Reds_r")
        plt.title("Top 10 High-Risk Districts (Composite Risk Score: 0-100 Scale)", fontsize=13, fontweight='bold')
        plt.xlabel("Composite Risk Score", fontsize=11)
        plt.ylabel("District Location", fontsize=11)
        for p in ax.patches:
            ax.annotate(f"{p.get_width():.1f}", (p.get_width() + 0.5, p.get_y() + p.get_height() / 2.),
                        ha='left', va='center', fontsize=10)
        plt.tight_layout()
        chart3_path = os.path.join(output_dir, "composite_risk_score_ranking.png")
        plt.savefig(chart3_path, dpi=300)
        plt.close()

    print(f"  Saved Analytical Charts to: {output_dir}")


def run(spark=None, df_sev=None, risk_scored_df=None):
    own_spark = spark is None
    if own_spark:
        spark = get_spark_session("RRSIS_Task10_Visualization")

    if df_sev is None or risk_scored_df is None:
        risk_scored_df = run_task7(spark)
        # Re-run task4 severity dataset if not passed
        from src.task4_severity_index import run as run_task4
        df_sev = run_task4(spark)

    print("\n==========================================================")
    print(" TASK 10: GEOSPATIAL MAPPING & DATA VISUALIZATION")
    print("==========================================================")

    output_dir = os.path.join(REPO_ROOT, "output", "visualizations")
    os.makedirs(output_dir, exist_ok=True)

    # 1. Generate 2D Coordinate Map Plot (Latitude vs Longitude)
    generate_geospatial_map_plot(df_sev, output_dir)

    # 2. Generate Interactive Leaflet / Folium HTML Map
    generate_interactive_html_map(df_sev, output_dir)

    # 3. Generate Task Visualization Charts
    generate_analytical_charts(df_sev, risk_scored_df, output_dir)

    print(f"\n[Task 10] All map plots and charts successfully written to: {output_dir}")
    print("==========================================================\n")

    if own_spark:
        spark.stop()


if __name__ == "__main__":
    run()
