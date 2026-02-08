import duckdb
import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "raw_data")
# We use the CLEAN data from Phase 1 for 2025 analysis
CLEAN_2025_DIR = os.path.join(BASE_DIR, "processed_data", "clean_data", "*.parquet")
RESULTS_DIR = os.path.join(BASE_DIR, "results")

# "Manhattan South of 60th St" (Source: NYC TLC Zone Map)
CONGESTION_ZONE_IDS = (
    12, 13, 43, 45, 48, 50, 68, 79, 87, 88, 90, 100, 107, 113, 114, 116, 120, 125, 
    137, 140, 141, 142, 143, 144, 148, 151, 152, 153, 158, 161, 162, 163, 164, 166, 
    170, 186, 209, 211, 224, 229, 230, 231, 232, 233, 234, 236, 237, 238, 239, 243, 
    244, 246, 249, 261, 262, 263
)

class ImpactAnalysis:
    def __init__(self):
        print("Initializing Phase 2: Congestion Impact Engine...")
        
        # --- FIX: CREATE RESULTS DIRECTORY IF MISSING ---
        if not os.path.exists(RESULTS_DIR):
            print(f"  > Creating missing directory: {RESULTS_DIR}")
            os.makedirs(RESULTS_DIR)
            
        self.con = duckdb.connect(database=':memory:')

    def audit_leakage(self):
        """
        Calculates Surcharge Compliance Rate for trips entering the zone after Jan 5, 2025.
        """
        print("  > Auditing Surcharge Leakage...")
        
        # Logic: Pickup OUTSIDE zone -> Dropoff INSIDE zone -> Date > Jan 5
        query = f"""
            WITH eligible_trips AS (
                SELECT 
                    pickup_loc,
                    dropoff_loc,
                    congestion_surcharge,
                    total_amount
                FROM read_parquet('{CLEAN_2025_DIR.replace('\\', '/')}')
                WHERE 
                    pickup_time >= '2025-01-05'
                    AND pickup_loc NOT IN {CONGESTION_ZONE_IDS}
                    AND dropoff_loc IN {CONGESTION_ZONE_IDS}
            )
            SELECT 
                COUNT(*) as total_eligible_trips,
                SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) as compliant_trips,
                
                -- Compliance Rate %
                (SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as compliance_rate_pct,
                
                -- Revenue Lost (Approximate: assuming $2.50 surcharge usually)
                (COUNT(*) - SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END)) * 2.75 as estimated_revenue_loss
            FROM eligible_trips
        """
        
        df_stats = self.con.execute(query).df()
        df_stats.to_csv(os.path.join(RESULTS_DIR, "audit_leakage_stats.csv"), index=False)
        print("    - Saved: audit_leakage_stats.csv")

        # Identify Top 3 Leaky Locations
        query_locs = f"""
            SELECT 
                pickup_loc,
                COUNT(*) as total_trips,
                SUM(CASE WHEN congestion_surcharge IS NULL OR congestion_surcharge = 0 THEN 1 ELSE 0 END) as missed_surcharges,
                (SUM(CASE WHEN congestion_surcharge IS NULL OR congestion_surcharge = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as leakage_rate_pct
            FROM read_parquet('{CLEAN_2025_DIR.replace('\\', '/')}')
            WHERE 
                pickup_time >= '2025-01-05'
                AND pickup_loc NOT IN {CONGESTION_ZONE_IDS}
                AND dropoff_loc IN {CONGESTION_ZONE_IDS}
            GROUP BY 1
            HAVING total_trips > 50 
            ORDER BY missed_surcharges DESC
            LIMIT 3
        """
        
        df_locs = self.con.execute(query_locs).df()
        df_locs.to_csv(os.path.join(RESULTS_DIR, "audit_top3_leakage_locs.csv"), index=False)
        print("    - Saved: audit_top3_leakage_locs.csv")

    def compare_q1_decline(self):
        """
        Compares Q1 2024 (Baseline) vs Q1 2025 (Post-Implementation)
        """
        print("  > Comparing Q1 2024 vs Q1 2025 Volumes...")
        
        try:
            # Fix path slashes for Windows
            raw_clean_path = RAW_DIR.replace('\\', '/')
            clean_2025_path = CLEAN_2025_DIR.replace('\\', '/')

            # 1. Load Q1 2024 Raw Data (Yellow + Green)
            # We use filename=True to avoid schema mismatches if files are slightly different
            q1_2024_query = f"""
                SELECT COUNT(*) as trip_count, '2024 Q1' as period
                FROM read_parquet([
                    '{raw_clean_path}/yellow_tripdata_2024-01.parquet',
                    '{raw_clean_path}/yellow_tripdata_2024-02.parquet',
                    '{raw_clean_path}/yellow_tripdata_2024-03.parquet',
                    '{raw_clean_path}/green_tripdata_2024-01.parquet',
                    '{raw_clean_path}/green_tripdata_2024-02.parquet',
                    '{raw_clean_path}/green_tripdata_2024-03.parquet'
                ], union_by_name=True)
                WHERE DOLocationID IN {CONGESTION_ZONE_IDS}
            """
            
            # 2. Load Q1 2025 Clean Data
            q1_2025_query = f"""
                SELECT COUNT(*) as trip_count, '2025 Q1' as period
                FROM read_parquet('{clean_2025_path}')
                WHERE 
                    dropoff_loc IN {CONGESTION_ZONE_IDS}
                    AND MONTH(pickup_time) IN (1, 2, 3)
            """
            
            # Combine
            final_query = f"""
                {q1_2024_query}
                UNION ALL
                {q1_2025_query}
            """
            
            df_decline = self.con.execute(final_query).df()
            
            # Calculate Percentage Drop
            if len(df_decline) == 2:
                # Ensure we grab the right rows regardless of order
                vol_24 = df_decline[df_decline['period'] == '2024 Q1']['trip_count'].values[0]
                vol_25 = df_decline[df_decline['period'] == '2025 Q1']['trip_count'].values[0]
                drop_pct = ((vol_25 - vol_24) / vol_24) * 100
                print(f"    ! Result: Traffic Volume Changed by {drop_pct:.2f}%")
            
            df_decline.to_csv(os.path.join(RESULTS_DIR, "impact_q1_comparison.csv"), index=False)
            print("    - Saved: impact_q1_comparison.csv")
            
        except Exception as e:
            print(f"    ! Error comparing Q1 data (Did you download 2024 files?): {e}")

    def run(self):
        self.audit_leakage()
        self.compare_q1_decline()
        print("SUCCESS: Phase 2 Analysis Complete.")

if __name__ == "__main__":
    pipeline = ImpactAnalysis()
    pipeline.run()