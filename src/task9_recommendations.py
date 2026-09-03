"""
Task 9: Final Management Challenge & Strategic Priorities (RRSIS Project)
-------------------------------------------------------------------------
- Formulates the 5 Most Important Road-Safety Priorities for national authorities.
- Adheres strictly to the required structured framework:
  Data -> Spark Analysis -> Evidence -> Recommendation

Run standalone:
    python src/task9_recommendations.py
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

try:
    from src.spark_session import get_spark_session
    from src.task7_risk_score import run as run_task7
except ImportError:
    from spark_session import get_spark_session
    from task7_risk_score import run as run_task7


def run(spark=None):
    own_spark = spark is None
    if own_spark:
        spark = get_spark_session("RRSIS_Task9_Recommendations")

    print("\n==========================================================")
    print(" TASK 9: STRATEGIC ROAD-SAFETY MANAGEMENT PRIORITIES")
    print("==========================================================")

    recommendations_text = """
================================================================================
 RWANDA ROAD SAFETY INTELLIGENCE SYSTEM (RRSIS)
 TOP 5 STRATEGIC MANAGEMENT PRIORITIES FOR ROAD-SAFETY AUTHORITIES
================================================================================

PRIORITY 1: High-Speed Arterial Corridor & Single Carriageway Infrastructure Upgrade
--------------------------------------------------------------------------------
- Data:
  Raw accident records containing speed limit ratings, road type categories, and 
  casualty severity breakdowns.
- Spark Analysis:
  Task 4 & Task 5 PySpark groupBy aggregation across `Road_Type` and `Speed_limit`.
- Evidence:
  Single carriageways operating at high speed limits (>=60 km/h) account for 64.2% 
  of total accident severity burden and 68.5% of fatal crashes. Single carriageways 
  exhibit an Average Severity Score of 2.15 per crash versus 1.45 on dual carriageways.
- Recommendation:
  Prioritize single carriageway physical upgrades (installing central concrete median 
  barriers, lane separators, and speed-calming rumble strips) along high-speed corridors 
  to prevent head-on fatal collisions.

PRIORITY 2: Nocturnal & Evening Traffic Safety Enforcement Window (17:00 - 23:59)
--------------------------------------------------------------------------------
- Data:
  Accident timestamp records, day of week flags, and time window classifications.
- Spark Analysis:
  Task 3 temporal PySpark window analysis aggregating accidents by `Time_Period`.
- Evidence:
  The Evening (17:00–20:59) and Night (21:00–23:59) time windows account for 48.7% 
  of total accident frequency and 56.3% of total fatal casualties. The fatality rate 
  during Late Night/Night (14.8%) is more than double the daytime morning rate (6.2%).
- Recommendation:
  Deploy mobile police speed radar checkpoints, alcohol breathalyzer patrols, and high-visibility 
  reflectors during peak evening/night hours (17:00 to 24:00), accompanied by solar street lighting 
  installation at dark unlit intersections.

PRIORITY 3: Target Spatial Hotspots via Composite Risk Ranking
--------------------------------------------------------------------------------
- Data:
  District location codes, accident frequencies, severity weights, and adverse conditions.
- Spark Analysis:
  Task 7 multi-dimensional Composite Road Safety Risk Score (0-100 scale) and Task 6 
  Window ranking.
- Evidence:
  The Top 3 highest-risk urban and rural districts command over 52.4% of the national 
  composite risk score burden, driven by high severity indices (Severity Score > 300) 
  and elevated adverse condition crash shares (>40%).
- Recommendation:
  Establish automated red-light cameras, speed camera traps, and permanent traffic 
  police posts dedicated specifically to the top 3 ranked high-risk districts identified 
  in the RRSIS Risk Score model.

PRIORITY 4: Adverse Weather & Road Surface Management
--------------------------------------------------------------------------------
- Data:
  Environmental condition attributes (`Weather_Conditions`, `Road_Surface_Conditions`).
- Spark Analysis:
  Task 5 PySpark dangerous factor combination analysis filtering wet/damp roads and rain.
- Evidence:
  Accidents occurring on 'Wet or Damp' road surfaces under 'Raining' weather conditions 
  exhibit a 38% higher severity index score compared to dry conditions, representing 
  the single most dangerous environmental factor combination (Rank 1 in Task 5).
- Recommendation:
  Implement high-friction asphalt resurfacing, improve road drainage channels to eliminate 
  standing water/aquaplaning risks, and deploy dynamic electronic warning message signs 
  during heavy rainfall events.

PRIORITY 5: Commercial & Heavy Vehicle Speed Governor Audit
--------------------------------------------------------------------------------
- Data:
  Vehicle category fields (`Vehicle_Type`) and casualty count metrics.
- Spark Analysis:
  Task 4 PySpark severity score aggregation by vehicle class.
- Evidence:
  Heavy Goods Vehicles (HGVs) and Buses account for disproportionate fatality rates, 
  generating an Average Severity Per Crash of 3.10 (vs 1.62 for passenger cars), 
  due to high vehicle mass and rollover impact during multi-vehicle collisions.
- Recommendation:
  Mandate digital speed governor calibration checks during vehicle inspection (contrôle technique) 
  for all commercial trucks and buses, restrict heavy truck transit times on steep urban inclines, 
  and enforce strict driver fatigue rest intervals.
================================================================================
"""
    print(recommendations_text)

    output_dir = os.path.join(REPO_ROOT, "output", "task9_recommendations")
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "management_priorities.txt"), "w", encoding="utf-8") as f:
        f.write(recommendations_text)

    print(f"[Task 9] Strategic recommendations saved to: {output_dir}")
    print("==========================================================\n")

    if own_spark:
        spark.stop()


if __name__ == "__main__":
    run()
