import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# We look for the CLEAN 2025 data (processed in Phase 1)
CLEAN_2025_DIR = os.path.join(BASE_DIR, "processed_data", "clean_data", "*.parquet")
RESULTS_DIR = os.path.join(BASE_DIR, "results")

class RainTaxAnalysis:
    def __init__(self):
        print("Initializing Phase 4: Rain Tax Engine...")
        self.con = duckdb.connect(database=':memory:')
        sns.set_theme(style="whitegrid")

    def generate_weather_data(self):
        """
        Simulates 2025 NYC Weather Data.
        """
        print("  > Fetching (Simulating) 2025 Precipitation Data...")
        dates = pd.date_range(start='2025-01-01', end='2025-12-31')
        
        # Simulation logic: 30% chance of rain
        np.random.seed(42)
        precip = []
        for d in dates:
            is_rainy = np.random.rand() < 0.30
            if is_rainy:
                amount = np.random.gamma(shape=2.0, scale=5.0)
            else:
                amount = 0.0
            precip.append(round(amount, 2))
            
        df_weather = pd.DataFrame({'date': dates, 'prcp_mm': precip})
        
        # FIX 1: Ensure this is a standard datetime format
        df_weather['date'] = pd.to_datetime(df_weather['date'])
        
        return df_weather

    def calculate_elasticity(self):
        print("  > Calculating Rain Elasticity of Demand...")
        
        # 1. Get Daily Trip Counts from DuckDB
        query = f"""
            SELECT 
                CAST(pickup_time AS DATE) as date,
                COUNT(*) as trip_count
            FROM read_parquet('{CLEAN_2025_DIR.replace('\\', '/')}')
            GROUP BY 1
            ORDER BY 1
        """
        df_trips = self.con.execute(query).df()
        
        # FIX 2: Force DuckDB's date column to match Pandas datetime format
        df_trips['date'] = pd.to_datetime(df_trips['date'])
        
        # 2. Join with Weather
        df_weather = self.generate_weather_data()
        
        # Now both keys are 'datetime64[ns]', so this merge will succeed
        df_merged = pd.merge(df_trips, df_weather, on='date', how='inner')
        
        # 3. Calculate Correlation
        correlation = df_merged['trip_count'].corr(df_merged['prcp_mm'])
        
        # 4. Save Data for Dashboard
        output_path = os.path.join(RESULTS_DIR, "weather_elasticity.csv")
        df_merged.to_csv(output_path, index=False)
        
        print(f"    - Correlation Coefficient: {correlation:.4f}")
        elasticity_type = "Inelastic (Rain has little effect)" if abs(correlation) < 0.3 else "Elastic"
        print(f"    - Interpretation: {elasticity_type}")
        
        # 5. Plot the "Wettest Month"
        df_merged['month'] = df_merged['date'].dt.month_name()
        wettest_month = df_merged.groupby('month')['prcp_mm'].sum().idxmax()
        
        df_plot = df_merged[df_merged['month'] == wettest_month]
        
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=df_plot, x="prcp_mm", y="trip_count", size="prcp_mm", sizes=(20, 200), color="#3498db")
        # Add trend line
        if len(df_plot) > 1:
            sns.regplot(data=df_plot, x="prcp_mm", y="trip_count", scatter=False, color="red")
        
        plt.title(f"Rain Elasticity: Trips vs Precip in {wettest_month} 2025", fontsize=14)
        plt.xlabel("Precipitation (mm)")
        plt.ylabel("Daily Trip Count")
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_DIR, "viz_rain_elasticity.png"))
        print("    - Saved: viz_rain_elasticity.png")

    def run(self):
        self.calculate_elasticity()

if __name__ == "__main__":
    analysis = RainTaxAnalysis()
    analysis.run()