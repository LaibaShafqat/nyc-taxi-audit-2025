import duckdb
import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CLEAN_2025_DIR = os.path.join(BASE_DIR, "processed_data", "clean_data", "*.parquet")
RESULTS_DIR = os.path.join(BASE_DIR, "results")

CONGESTION_ZONE_IDS = (12, 13, 43, 45, 48, 50, 68, 79, 87, 88, 90, 100, 107, 113, 114, 116, 120, 125, 137, 140, 141, 142, 143, 144, 148, 151, 152, 153, 158, 161, 162, 163, 164, 166, 170, 186, 209, 211, 224, 229, 230, 231, 232, 233, 234, 236, 237, 238, 239, 243, 244, 246, 249, 261, 262, 263)

class AnalysisPipeline:
    def __init__(self):
        print("Initializing Phase 2: Core Analysis Engine...")
        self.con = duckdb.connect(database=':memory:')
        self.clean_path = CLEAN_2025_DIR.replace('\\', '/')
        if not os.path.exists(RESULTS_DIR): os.makedirs(RESULTS_DIR)

    def analyze_revenue(self):
        print("  > Analyzing Revenue & Traffic...")
        query = f"SELECT SUM(total_amount) as total_revenue, SUM(congestion_surcharge) as total_surcharge, COUNT(*) as total_rides FROM read_parquet('{self.clean_path}')"
        self.con.execute(query).df().to_csv(os.path.join(RESULTS_DIR, "summary_revenue.csv"), index=False)

    def analyze_fairness(self):
        print("  > Analyzing Fairness...")
        query = f"SELECT AVG(CASE WHEN fare > 0 THEN (total_amount - fare - congestion_surcharge) / fare ELSE 0 END) * 100 as avg_tip_percent, SUM(CASE WHEN trip_distance < 2 AND dropoff_loc IN {CONGESTION_ZONE_IDS} THEN 1 ELSE 0 END) as short_trip_count FROM read_parquet('{self.clean_path}') WHERE fare > 0"
        self.con.execute(query).df().to_csv(os.path.join(RESULTS_DIR, "summary_fairness.csv"), index=False)

    def analyze_fraud(self):
        print("  > Auditing for Fraud Types...")
        # This one groups by VIOLATION TYPE
        query = f"""
            SELECT 
                CASE 
                    WHEN (trip_distance / (CASE WHEN (date_part('epoch', dropoff_time) - date_part('epoch', pickup_time))/3600.0 = 0 THEN 1 ELSE (date_part('epoch', dropoff_time) - date_part('epoch', pickup_time))/3600.0 END)) > 100 THEN 'Teleporter (>100mph)'
                    WHEN trip_distance = 0 AND congestion_surcharge > 0 THEN 'Stationary Charge'
                    ELSE 'Other'
                END as violation_type,
                COUNT(*) as violation_count
            FROM read_parquet('{self.clean_path}')
            WHERE 
                (trip_distance / (CASE WHEN (date_part('epoch', dropoff_time) - date_part('epoch', pickup_time))/3600.0 = 0 THEN 1 ELSE (date_part('epoch', dropoff_time) - date_part('epoch', pickup_time))/3600.0 END)) > 100
                OR (trip_distance = 0 AND congestion_surcharge > 0)
            GROUP BY 1
            ORDER BY 2 DESC
        """
        self.con.execute(query).df().to_csv(os.path.join(RESULTS_DIR, "summary_fraud.csv"), index=False)

    # --- NEW FUNCTION FOR TOP 5 LIST ---
    def analyze_suspicious_vendors(self):
        print("  > Identifying Top 5 Suspicious Vendors...")
        query = f"""
            SELECT 
                pickup_loc as VendorID, 
                COUNT(*) as suspicious_trips
            FROM read_parquet('{self.clean_path}')
            WHERE 
                (trip_distance / (CASE WHEN (date_part('epoch', dropoff_time) - date_part('epoch', pickup_time))/3600.0 = 0 THEN 1 ELSE (date_part('epoch', dropoff_time) - date_part('epoch', pickup_time))/3600.0 END)) > 100
                OR (trip_distance = 0 AND congestion_surcharge > 0)
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 5
        """
        df = self.con.execute(query).df()
        df.to_csv(os.path.join(RESULTS_DIR, "audit_suspicious_vendors.csv"), index=False)
        print("    - Saved: audit_suspicious_vendors.csv")

    def run(self):
        self.analyze_revenue()
        self.analyze_fairness()
        self.analyze_fraud()
        self.analyze_suspicious_vendors() # Run the new function
        print("SUCCESS: Phase 2 Core Analysis Complete.")

if __name__ == "__main__":
    pipeline = AnalysisPipeline()
    pipeline.run()