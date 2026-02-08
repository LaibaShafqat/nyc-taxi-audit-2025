import duckdb
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "raw_data")
CLEAN_2025_DIR = os.path.join(BASE_DIR, "processed_data", "clean_data", "*.parquet")
RESULTS_DIR = os.path.join(BASE_DIR, "results")

# Geospatial Definitions
CONGESTION_ZONE_IDS = (12, 13, 43, 45, 48, 50, 68, 79, 87, 88, 90, 100, 107, 113, 114, 116, 120, 125, 137, 140, 141, 142, 143, 144, 148, 151, 152, 153, 158, 161, 162, 163, 164, 166, 170, 186, 209, 211, 224, 229, 230, 231, 232, 233, 234, 236, 237, 238, 239, 243, 244, 246, 249, 261, 262, 263)
BORDER_ZONES = (238, 239, 263, 262, 236, 237, 74, 75, 142, 143, 43, 48, 50, 100, 161, 162, 163, 164, 230)

class VisualAudit:
    def __init__(self):
        print("Initializing Phase 3: Visual Audit Engine...")
        self.con = duckdb.connect(database=':memory:')
        sns.set_theme(style="whitegrid")
        self.raw_path = RAW_DIR.replace('\\', '/')
        self.clean_path = CLEAN_2025_DIR.replace('\\', '/')

    def plot_border_effect(self):
        print("  > Generating 'Border Effect' Data & Image...")
        
        q_24 = f"""
            SELECT DOLocationID as ZoneID, COUNT(*) as count_2024
            FROM read_parquet([
                '{self.raw_path}/yellow_tripdata_2024-01.parquet', '{self.raw_path}/yellow_tripdata_2024-02.parquet', '{self.raw_path}/yellow_tripdata_2024-03.parquet',
                '{self.raw_path}/green_tripdata_2024-01.parquet', '{self.raw_path}/green_tripdata_2024-02.parquet', '{self.raw_path}/green_tripdata_2024-03.parquet'
            ], union_by_name=True)
            WHERE DOLocationID IN {BORDER_ZONES} GROUP BY 1
        """
        q_25 = f"""
            SELECT dropoff_loc as ZoneID, COUNT(*) as count_2025
            FROM read_parquet('{self.clean_path}')
            WHERE dropoff_loc IN {BORDER_ZONES} AND MONTH(pickup_time) IN (1, 2, 3) GROUP BY 1
        """
        
        final_query = f"""
            WITH t24 AS ({q_24}), t25 AS ({q_25})
            SELECT 
                t24.ZoneID, 
                ((count_2025 - count_2024) / count_2024::FLOAT) * 100 as pct_change,
                CASE WHEN t24.ZoneID IN {CONGESTION_ZONE_IDS} THEN 'Inside Zone' ELSE 'Outside Zone' END as location_type
            FROM t24 JOIN t25 ON t24.ZoneID = t25.ZoneID
            ORDER BY pct_change DESC
        """
        df = self.con.execute(final_query).df()
        
        # SAVE CSV FOR MAP
        df.to_csv(os.path.join(RESULTS_DIR, "border_effect.csv"), index=False)

        # Plot Image
        plt.figure(figsize=(12, 6))
        palette = {"Inside Zone": "#e74c3c", "Outside Zone": "#3498db"}
        sns.barplot(data=df, x="ZoneID", y="pct_change", hue="location_type", palette=palette, dodge=False)
        plt.axhline(0, color='black', linewidth=1)
        plt.title("Border Effect: % Change in Drop-offs (Q1 2024 vs Q1 2025)", fontsize=14)
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_DIR, "viz_border_effect.png"))
        print("    - Saved: viz_border_effect.png + border_effect.csv")

    def _save_heatmap_img(self, df, filename, title):
        pivot = df.pivot(index="day_num", columns="hour_num", values="avg_speed")
        pivot.index = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        plt.figure(figsize=(10, 6)) 
        sns.heatmap(pivot, cmap="magma", annot=False, fmt=".1f", vmin=5, vmax=20)
        plt.title(title, fontsize=12)
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_DIR, filename))
        print(f"    - Saved: {filename}")

    def plot_velocity_heatmaps(self):
        print("  > Generating 'Before vs After' Velocity Heatmaps...")
        
        # 1. BEFORE (2024)
        q_24 = f"""
            SELECT isodow(tpep_pickup_datetime) as day_num, extract(hour from tpep_pickup_datetime) as hour_num,
            AVG(trip_distance / (date_part('epoch', tpep_dropoff_datetime) - date_part('epoch', tpep_pickup_datetime)) * 3600) as avg_speed
            FROM read_parquet(['{self.raw_path}/yellow_tripdata_2024-01.parquet', '{self.raw_path}/yellow_tripdata_2024-02.parquet', '{self.raw_path}/yellow_tripdata_2024-03.parquet'])
            WHERE PULocationID IN {CONGESTION_ZONE_IDS} AND DOLocationID IN {CONGESTION_ZONE_IDS}
            AND trip_distance > 0.5 
            AND (date_part('epoch', tpep_dropoff_datetime) - date_part('epoch', tpep_pickup_datetime)) > 60
            GROUP BY 1, 2
        """
        df_24 = self.con.execute(q_24).df()
        self._save_heatmap_img(df_24, "viz_velocity_2024.png", "Congestion Velocity: Q1 2024 (Baseline)")

        # 2. AFTER (2025)
        q_25 = f"""
            SELECT isodow(pickup_time) as day_num, extract(hour from pickup_time) as hour_num,
            AVG(trip_distance / (date_part('epoch', dropoff_time) - date_part('epoch', pickup_time)) * 3600) as avg_speed
            FROM read_parquet('{self.clean_path}')
            WHERE pickup_loc IN {CONGESTION_ZONE_IDS} AND dropoff_loc IN {CONGESTION_ZONE_IDS}
            AND MONTH(pickup_time) IN (1, 2, 3) AND trip_distance > 0.5
            AND (date_part('epoch', dropoff_time) - date_part('epoch', pickup_time)) > 60
            GROUP BY 1, 2
        """
        df_25 = self.con.execute(q_25).df()
        self._save_heatmap_img(df_25, "viz_velocity_2025.png", "Congestion Velocity: Q1 2025 (Post-Toll)")
        
        # Save MAIN heatmap for dashboard default
        self._save_heatmap_img(df_25, "viz_velocity_heatmap.png", "Congestion Velocity: Q1 2025 (Post-Toll)")

    def plot_crowding_out(self):
        print("  > Generating 'Crowding Out' Analysis (FORCE VISIBILITY MODE)...")
        query = f"""
            SELECT strftime(date_trunc('month', pickup_time), '%Y-%m') as month_str,
            AVG(COALESCE(congestion_surcharge, 0)) as avg_surcharge,
            AVG(CASE WHEN fare > 0 THEN (COALESCE(total_amount, 0) - COALESCE(fare, 0) - COALESCE(congestion_surcharge, 0)) / COALESCE(fare, 1) ELSE 0 END) * 100 as avg_tip_pct
            FROM read_parquet('{self.clean_path}')
            WHERE fare > 0 AND pickup_time >= '2024-01-01' AND pickup_time < '2026-01-01'
            GROUP BY 1 ORDER BY 1
        """
        df = self.con.execute(query).df()
        
        # DEBUG PRINT: Verify data exists
        print("    [DEBUG] Tip Data Preview:")
        print(df[['month_str', 'avg_tip_pct']].head())

        fig, ax1 = plt.subplots(figsize=(12, 6))
        
        # 1. Plot Blue Bars (Surcharge) - Semi-transparent so they don't hide anything
        color = 'tab:blue'
        ax1.set_xlabel('Month')
        ax1.set_ylabel('Avg Surcharge ($)', color=color, fontweight='bold')
        ax1.bar(df['month_str'], df['avg_surcharge'], color=color, alpha=0.4, label='Surcharge')
        ax1.tick_params(axis='y', labelcolor=color)
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # 2. Plot Red Line (Tips) - Secondary Axis
        ax2 = ax1.twinx()
        color = 'tab:red'
        ax2.set_ylabel('Avg Tip %', color=color, fontweight='bold')
        
        # FORCE SCALE: Ensure the line doesn't disappear if values are small
        # We set the limit from 0 to 120% of the max value to keep it centered
        max_tip = df['avg_tip_pct'].max()
        if max_tip > 0:
            ax2.set_ylim(0, max_tip * 1.2)
        
        # FORCE Z-ORDER: Draw line ON TOP of everything (zorder=10)
        ax2.plot(df['month_str'], df['avg_tip_pct'], color=color, marker='o', 
                 linewidth=4, markersize=8, label='Tip %', zorder=10)
        ax2.tick_params(axis='y', labelcolor=color)
        
        # TRICK: Ensure ax2 background is transparent so ax1 bars show through
        ax2.patch.set_visible(False)
        ax1.set_zorder(1)
        ax2.set_zorder(10)
        
        plt.title("Crowding Out Effect: Surcharge vs Tip %", fontsize=14)
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_DIR, "viz_crowding_out.png"))
        print("    - Saved: viz_crowding_out.png")

    def run(self):
        self.plot_border_effect()
        self.plot_velocity_heatmaps()
        self.plot_crowding_out()

if __name__ == "__main__":
    audit = VisualAudit()
    audit.run()