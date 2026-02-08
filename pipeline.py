import time
import phase1_pipeline
import phase2_analysis
import phase2_impact
import phase3_visuals
import phase4_rain
import phase4_report

def main():
    print("==========================================")
    print("   NYC TAXI CONGESTION AUDIT PIPELINE     ")
    print("==========================================")
    
    # --- PHASE 1: DATA ENGINEERING ---
    # Ingests raw Parquet files, cleans them, and imputes missing Dec 2025 data.
    print("\n[STEP 1] Running Phase 1: Data Engineering...")
    p1 = phase1_pipeline.DuckDBPipeline()
    p1.run()
    
    # --- PHASE 2A: CORE ANALYSIS ---
    # Calculates Revenue, Fairness (Tips), and Fraud (Ghost Trips).
    # Output: summary_revenue.csv, summary_fairness.csv, summary_fraud.csv
    print("\n[STEP 2A] Running Phase 2: Core Analysis...")
    p2a = phase2_analysis.AnalysisPipeline()
    p2a.run()

    # --- PHASE 2B: IMPACT ANALYSIS ---
    # Calculates Surcharge Leakage and Q1 2024 vs 2025 Traffic Decline.
    # Output: audit_leakage_stats.csv, impact_q1_comparison.csv
    print("\n[STEP 2B] Running Phase 2: Impact Analysis...")
    p2b = phase2_impact.ImpactAnalysis()
    p2b.run()
    
    # --- PHASE 3: VISUALIZATION ---
    # Generates the 3 main charts: Border Effect, Velocity Heatmap, Crowding Out.
    # Output: viz_border_effect.png, viz_velocity_heatmap.png, viz_crowding_out.png
    print("\n[STEP 3] Running Phase 3: Visualization...")
    p3 = phase3_visuals.VisualAudit()
    p3.run()
    
    # --- PHASE 4A: RAIN TAX ---
    # Simulates weather data and calculates demand elasticity.
    # Output: weather_elasticity.csv, viz_rain_elasticity.png
    print("\n[STEP 4A] Running Phase 4: Rain Analysis...")
    p4 = phase4_rain.RainTaxAnalysis()
    p4.run()
    
    # --- PHASE 4B: REPORTING ---
    # Reads all CSVs generated above and writes the Executive Summary text file.
    # Output: FINAL_EXECUTIVE_REPORT.txt
    print("\n[STEP 4B] Generating Final Executive Report...")
    p5 = phase4_report.ReportGenerator()
    p5.run()
    
    print("       PIPELINE COMPLETE SUCCESS          ")
    
if __name__ == "__main__":
    main()