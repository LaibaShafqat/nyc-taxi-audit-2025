import duckdb
import os
import glob
import shutil

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, "raw_data")
OUTPUT_DIR = os.path.join(BASE_DIR, "processed_data")

class DuckDBPipeline:
    def __init__(self):
        print("Initializing DuckDB (No Java required)...")
        self.con = duckdb.connect(database=':memory:') # In-memory DB

    def process_batch(self, filepath, filename):
        print(f"  > Processing: {filename}...")
        
        # Determine strict column names based on file type
        if "yellow" in filename:
            taxi_type = "yellow"
            pickup_col = "tpep_pickup_datetime"
            dropoff_col = "tpep_dropoff_datetime"
        else:
            taxi_type = "green"
            pickup_col = "lpep_pickup_datetime"
            dropoff_col = "lpep_dropoff_datetime"
        
        # 1. We construct the query dynamically using the correct columns
        # 2. We use {{double braces}} for logic we want to insert LATER (clean/audit)
        query = f"""
        WITH raw_data AS (
            SELECT 
                {pickup_col} AS pickup_time,
                {dropoff_col} AS dropoff_time,
                PULocationID AS pickup_loc,
                DOLocationID AS dropoff_loc,
                trip_distance,
                fare_amount AS fare,
                total_amount,
                congestion_surcharge,
                '{taxi_type}' AS taxi_type
            FROM read_parquet('{filepath.replace('\\', '/')}')
        ),
        metrics AS (
            SELECT *,
                (date_part('epoch', dropoff_time) - date_part('epoch', pickup_time)) / 3600.0 AS duration_hrs
            FROM raw_data
        ),
        calc AS (
            SELECT *,
                CASE WHEN duration_hrs > 0 THEN trip_distance / duration_hrs ELSE 0 END AS speed_mph
            FROM metrics
        ),
        flagged AS (
            SELECT *,
                (speed_mph > 65 AND trip_distance > 1) AS is_physics_fail,
                (duration_hrs < (1.0/60.0) AND fare > 20) AS is_teleporter,
                (trip_distance = 0 AND fare > 0) AS is_stationary
            FROM calc
        )
        -- Split and Export
        SELECT * EXCLUDE (duration_hrs, speed_mph, is_physics_fail, is_teleporter, is_stationary)
        FROM flagged
        WHERE {{condition_logic}}
        """

        # Export CLEAN Data
        # We replace {{condition_logic}} with the actual filter
        clean_logic = "NOT (is_physics_fail OR is_teleporter OR is_stationary)"
        clean_file = os.path.join(OUTPUT_DIR, "clean_data", filename).replace('\\', '/')
        self.con.execute(f"COPY ({query.replace('{condition_logic}', clean_logic)}) TO '{clean_file}' (FORMAT PARQUET)")

        # Export AUDIT Data
        audit_logic = "(is_physics_fail OR is_teleporter OR is_stationary)"
        audit_file = os.path.join(OUTPUT_DIR, "audit_log", filename).replace('\\', '/')
        self.con.execute(f"COPY ({query.replace('{condition_logic}', audit_logic)}) TO '{audit_file}' (FORMAT PARQUET)")

    def impute_december(self):
        print("\n--- Generating Missing December 2025 Data ---")
        try:
            # Fix path slashes for DuckDB
            input_clean = INPUT_DIR.replace('\\', '/')
            output_file = os.path.join(OUTPUT_DIR, "clean_data", "imputed_dec_2025.parquet").replace('\\', '/')
            
            # For imputation, we read MANY files at once, so we MUST use COALESCE 
            # and union_by_name=True because the schema will be a mix of both.
            impute_sql = f"""
            COPY (
                SELECT 
                    COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) + INTERVAL 2 YEAR AS pickup_time,
                    COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) + INTERVAL 2 YEAR AS dropoff_time,
                    PULocationID AS pickup_loc, 
                    DOLocationID AS dropoff_loc, 
                    trip_distance, 
                    fare_amount AS fare, 
                    total_amount, 
                    congestion_surcharge, 
                    CASE WHEN filename LIKE '%yellow%' THEN 'yellow' ELSE 'green' END AS taxi_type
                FROM read_parquet('{input_clean}/*_tripdata_2023-12.parquet', union_by_name=True, filename=True)
                USING SAMPLE 30%
                
                UNION ALL
                
                SELECT 
                    COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) + INTERVAL 1 YEAR AS pickup_time,
                    COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) + INTERVAL 1 YEAR AS dropoff_time,
                    PULocationID AS pickup_loc, 
                    DOLocationID AS dropoff_loc, 
                    trip_distance, 
                    fare_amount AS fare, 
                    total_amount, 
                    congestion_surcharge, 
                    CASE WHEN filename LIKE '%yellow%' THEN 'yellow' ELSE 'green' END AS taxi_type
                FROM read_parquet('{input_clean}/*_tripdata_2024-12.parquet', union_by_name=True, filename=True)
                USING SAMPLE 70%
            ) TO '{output_file}' (FORMAT PARQUET)
            """
            
            self.con.execute(impute_sql)
            print("  > Imputation Complete: imputed_dec_2025.parquet created.")
            
        except Exception as e:
            print(f"Imputation Error: {e}")

    def run(self):
        print("--- DuckDB Phase 1 Pipeline Started ---")
        
        # Reset Output Dirs
        if os.path.exists(OUTPUT_DIR): shutil.rmtree(OUTPUT_DIR)
        os.makedirs(os.path.join(OUTPUT_DIR, "clean_data"))
        os.makedirs(os.path.join(OUTPUT_DIR, "audit_log"))

        # 1. Process 2025 Files
        files = sorted(glob.glob(os.path.join(INPUT_DIR, "*_tripdata_2025-*.parquet")))
        print(f"Found {len(files)} files to process.")
        
        for f in files:
            # We fix the slashes here to ensure Windows compatibility
            self.process_batch(f, os.path.basename(f))

        # 2. Check & Impute December
        if not any("2025-12" in f for f in files):
            self.impute_december()

        print(f"\nSUCCESS: Data processed to {OUTPUT_DIR}")

if __name__ == "__main__":
    pipeline = DuckDBPipeline()
    pipeline.run()