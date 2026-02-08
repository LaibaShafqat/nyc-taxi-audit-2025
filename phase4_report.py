import pandas as pd
import os
from fpdf import FPDF

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BASE_DIR, "results")
REPORT_PATH = os.path.join(BASE_DIR, "audit_report.pdf")

class PDFReport(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 15)
        self.cell(0, 10, 'NYC Congestion Pricing Audit (2025)', 0, 1, 'C')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

    def chapter_title(self, label):
        self.set_font('Arial', 'B', 12)
        self.set_fill_color(200, 220, 255)
        self.cell(0, 6, label, 0, 1, 'L', 1)
        self.ln(4)

    def chapter_body(self, body):
        self.set_font('Arial', '', 11)
        self.multi_cell(0, 5, body)
        self.ln()

class ReportGenerator:
    def __init__(self):
        print("Initializing Phase 4: PDF Reporting Engine...")
    
    def load_csv(self, filename):
        path = os.path.join(RESULTS_DIR, filename)
        if os.path.exists(path):
            return pd.read_csv(path)
        return None

    def run(self):
        pdf = PDFReport()
        pdf.add_page()
        
        # --- SECTION 1 ---
        pdf.chapter_title("1. Executive Summary: Financial & Traffic Impact")
        df_rev = self.load_csv("summary_revenue.csv")
        df_q1 = self.load_csv("impact_q1_comparison.csv")
        text = ""
        if df_rev is not None:
            rev = df_rev['total_revenue'].sum()
            surcharge = df_rev['total_surcharge'].sum()
            text += f"Total Revenue Generated: ${rev:,.2f}\n"
            text += f"Total Surcharges Collected: ${surcharge:,.2f}\n"
        if df_q1 is not None and len(df_q1) == 2:
            vol_24 = df_q1[df_q1['period'] == '2024 Q1']['trip_count'].values[0]
            vol_25 = df_q1[df_q1['period'] == '2025 Q1']['trip_count'].values[0]
            pct = ((vol_25 - vol_24) / vol_24) * 100
            text += f"\nTraffic Volume Change (Q1 2024 vs 2025): {pct:.2f}%\n"
            text += f"Verdict: {'SUCCESS' if pct < 0 else 'FAILURE (Traffic Increased)'}"
        pdf.chapter_body(text)

        # --- SECTION 2 ---
        pdf.chapter_title("2. Equity & Economics")
        df_fair = self.load_csv("summary_fairness.csv")
        text = ""
        if df_fair is not None:
            avg_tip = df_fair['avg_tip_percent'].mean()
            text += f"Average Driver Tip: {avg_tip:.2f}%\n"
            text += "Insight: High volume of short trips suggests demand is inelastic."
        pdf.chapter_body(text)

        # --- SECTION 3 (UPDATED) ---
        pdf.chapter_title("3. System Integrity (Fraud & Leakage)")
        df_leak = self.load_csv("audit_leakage_stats.csv")
        df_fraud = self.load_csv("summary_fraud.csv")
        df_vendors = self.load_csv("audit_suspicious_vendors.csv") # Load new file
        
        text = ""
        if df_leak is not None:
            rate = df_leak['compliance_rate_pct'].values[0]
            text += f"Surcharge Compliance Rate: {rate:.2f}%\n"
        
        if df_fraud is not None:
            ghost_trips = df_fraud['violation_count'].sum()
            if not df_fraud.empty:
                top_type = df_fraud.iloc[0]['violation_type']
                text += f"\nTotal 'Ghost Trips' Flagged: {ghost_trips:,}\n"
                text += f"Most Common Fraud Type: {top_type}\n"

        # ADDED: The Top 5 Vendors List
        if df_vendors is not None and not df_vendors.empty:
            text += "\n--- TOP 5 SUSPICIOUS VENDORS ---\n"
            for index, row in df_vendors.iterrows():
                text += f"{index+1}. LocationID {int(row['VendorID'])}: {int(row['suspicious_trips'])} suspicious trips\n"

        pdf.chapter_body(text)
        
        # --- SECTION 4 ---
        pdf.chapter_title("4. Weather Elasticity (The Rain Tax)")
        df_rain = self.load_csv("weather_elasticity.csv")
        text = ""
        if df_rain is not None:
            corr = df_rain['trip_count'].corr(df_rain['prcp_mm'])
            verdict = "INELASTIC" if abs(corr) < 0.3 else "ELASTIC"
            text += f"Rain Elasticity Score: {corr:.4f}\n"
            text += f"Verdict: {verdict}\n"
        pdf.chapter_body(text)

        pdf.output(REPORT_PATH)
        print(f"SUCCESS: PDF Report generated at: {REPORT_PATH}")

if __name__ == "__main__":
    gen = ReportGenerator()
    gen.run()