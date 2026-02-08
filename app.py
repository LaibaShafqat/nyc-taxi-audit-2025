import streamlit as st
import pandas as pd
import plotly.express as px
import os
import folium
import requests
from streamlit_folium import st_folium
from PIL import Image

st.set_page_config(page_title="NYC Congestion Audit 2025", layout="wide")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BASE_DIR, "results")

# --- CACHING FUNCTION (Prevents re-downloading map every time) ---
@st.cache_data
def get_nyc_geojson():
    # Official NYC OpenData URL
    url = "https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON"
    try:
        r = requests.get(url)
        if r.status_code == 200:
            return r.json()
    except:
        return None
    return None

def load_data(filename):
    path = os.path.join(RESULTS_DIR, filename)
    if os.path.exists(path):
        return pd.read_csv(path)
    return None

# --- HEADER & KPI SECTION ---
st.title("🚖 NYC Congestion Pricing Audit (2025)")
st.markdown("### Executive Dashboard | Client: NYC Dept of Transportation")
st.markdown("---")

# Load Summary Data for KPIs
df_rev = load_data("summary_revenue.csv")
df_leak = load_data("audit_leakage_stats.csv")

col1, col2, col3, col4 = st.columns(4)

if df_rev is not None:
    total_rev = df_rev['total_revenue'].sum()
    total_surcharge = df_rev['total_surcharge'].sum()
    total_rides = df_rev['total_rides'].sum()
    
    col1.metric("Total Revenue", f"${total_rev/1_000_000:.1f}M")
    col2.metric("Surcharges", f"${total_surcharge/1_000_000:.1f}M")
    col3.metric("Total Trips", f"{total_rides/1_000_000:.1f}M")

if df_leak is not None:
    compliance_rate = df_leak['compliance_rate_pct'].iloc[0]
    col4.metric("Compliance Rate", f"{compliance_rate:.1f}%")

st.markdown("---")

# --- TABS ---
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🗺️ The Map (Border Effect)", 
    "📉 The Flow (Velocity)", 
    "💰 The Economics (Tips)", 
    "☔ The Weather (Rain Tax)",
    "🚨 Fraud Audit (Ghost Trips)"
])

# --- TAB 1: INTERACTIVE MAP (FIXED SOURCE) ---
with tab1:
    st.header("The Border Effect: Zone Avoidance")
    st.write("Zones colored by % change in drop-offs. **Blue = Increase (Avoidance), Red = Decrease.**")
    
    col_map_a, col_map_b = st.columns([3, 1])
    
    with col_map_a:
        df_border = load_data("border_effect.csv")
        nyc_geo = get_nyc_geojson() # Use the cached downloader
        
        map_success = False
        
        if df_border is not None and nyc_geo is not None:
            try:
                # Ensure ZoneID is string for matching
                df_border['ZoneID'] = df_border['ZoneID'].astype(str)
                
                m = folium.Map(location=[40.7644, -73.975], zoom_start=11)
                
                folium.Choropleth(
                    geo_data=nyc_geo,
                    name="choropleth",
                    data=df_border,
                    columns=["ZoneID", "pct_change"],
                    # IMPORTANT: NYC OpenData uses 'location_id', not 'objectid'
                    key_on="feature.properties.location_id", 
                    fill_color="RdBu",
                    fill_opacity=0.7,
                    line_opacity=0.2,
                    legend_name="% Change"
                ).add_to(m)
                
                st_folium(m, width=900, height=500)
                map_success = True
            except Exception as e:
                st.error(f"Map Error: {e}")
        
        # Fallback if map fails
        if not map_success:
            st.warning("⚠️ Interactive map unavailable. Displaying static chart.")
            img_path = os.path.join(RESULTS_DIR, "viz_border_effect.png")
            if os.path.exists(img_path):
                st.image(Image.open(img_path), use_container_width=True)

    with col_map_b:
        st.subheader("Top Leaky Zones")
        df_locs = load_data("audit_top3_leakage_locs.csv")
        if df_locs is not None:
            st.dataframe(df_locs, hide_index=True)

# --- TAB 2: VELOCITY ---
with tab2:
    st.header("The Flow: Traffic Speed & Volume")
    
    df_q1 = load_data("impact_q1_comparison.csv")
    if df_q1 is not None and len(df_q1) == 2:
        vol_24 = df_q1[df_q1['period'] == '2024 Q1']['trip_count'].values[0]
        vol_25 = df_q1[df_q1['period'] == '2025 Q1']['trip_count'].values[0]
        pct_change = ((vol_25 - vol_24) / vol_24) * 100
        st.info(f"📉 **Traffic Volume Impact:** Q1 Traffic changed by **{pct_change:.2f}%** compared to 2024.")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("2024 (Baseline)")
        img_24 = os.path.join(RESULTS_DIR, "viz_velocity_2024.png")
        if os.path.exists(img_24):
            st.image(Image.open(img_24), use_container_width=True)
    with col2:
        st.subheader("2025 (Post-Toll)")
        img_25 = os.path.join(RESULTS_DIR, "viz_velocity_2025.png")
        img_main = os.path.join(RESULTS_DIR, "viz_velocity_heatmap.png")
        if os.path.exists(img_25):
            st.image(Image.open(img_25), use_container_width=True)
        elif os.path.exists(img_main):
            st.image(Image.open(img_main), caption="2025 Heatmap", use_container_width=True)

# --- TAB 3: ECONOMICS ---
with tab3:
    st.header("The Economics: Driver Tips vs. Surcharges")
    
    col_c, col_d = st.columns([3, 1])
    with col_c:
        img_path = os.path.join(RESULTS_DIR, "viz_crowding_out.png")
        if os.path.exists(img_path):
            st.image(Image.open(img_path), caption="Crowding Out Effect (Blue=Fee, Red=Tip)", use_container_width=True)
            
    with col_d:
        st.subheader("Fairness Analysis")
        df_fair = load_data("summary_fairness.csv")
        if df_fair is not None:
            avg_tip = df_fair['avg_tip_percent'].mean()
            st.metric("Avg Tip % (2025)", f"{avg_tip:.1f}%")

# --- TAB 4: WEATHER ---
with tab4:
    st.header("The Weather: Rain Elasticity")
    csv_path = os.path.join(RESULTS_DIR, "weather_elasticity.csv")
    
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        col_e, col_f = st.columns([3, 1])
        with col_e:
            try:
                fig = px.scatter(df, x="prcp_mm", y="trip_count", trendline="ols", 
                                title="Daily Trip Volume vs. Rainfall (2025)")
            except:
                fig = px.scatter(df, x="prcp_mm", y="trip_count", 
                                title="Daily Trip Volume vs. Rainfall (2025)")
            st.plotly_chart(fig, use_container_width=True)
        with col_f:
            corr = df['trip_count'].corr(df['prcp_mm'])
            st.metric("Elasticity Score", f"{corr:.3f}")
            st.write("Verdict: **INELASTIC**" if abs(corr) < 0.3 else "Verdict: **ELASTIC**")

# --- TAB 5: FRAUD AUDIT ---
with tab5:
    st.header("🚨 System Integrity: Ghost Trip Audit")
    df_fraud = load_data("summary_fraud.csv")
    if df_fraud is not None:
        fig_fraud = px.bar(df_fraud, x="violation_count", y="violation_type", 
                           orientation='h', color="violation_type", title="Suspicious Trips Detected")
        st.plotly_chart(fig_fraud, use_container_width=True)
        
        st.subheader("Top Suspicious Vendors")
        df_vendors = load_data("audit_suspicious_vendors.csv")
        if df_vendors is not None:
            st.dataframe(df_vendors, hide_index=True)