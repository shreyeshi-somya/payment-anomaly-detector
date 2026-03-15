import streamlit as st
import pandas as pd
import sys
sys.path.append('/app/src')
from llm_explainer.explainer import explain_anomaly

st.set_page_config(layout="wide")
st.markdown("""
    <style>
    .stButton > button {
        background-color: #ff4b4b;
        color: white;
    }
    </style>
""", unsafe_allow_html=True)

st.title("Payment Anomaly Detector")

# Load data
anomalies = pd.read_parquet("/app/data/anomalies")
drilldown = pd.read_parquet("/app/data/drilldown")

# Show only daily-level anomalies
daily_anomalies = anomalies[anomalies['level'] == 'daily_total'].copy()

# Filter to key anomalies
key_dates = [
    ('2024-08-01', 'avg_decline_rate'),
    ('2025-02-01', 'total_transactions'),
    ('2025-06-01', 'avg_latency'),
    ('2024-12-01', 'total_transactions'),
    ('2025-12-01', 'total_transactions'),
]

daily_anomalies['date_str'] = daily_anomalies['transaction_date'].astype(str)
daily_anomalies = daily_anomalies[
    daily_anomalies.apply(lambda x: (x['date_str'], x['metric']) in key_dates, axis=1)
].copy()

daily_anomalies['key'] = daily_anomalies['transaction_date'].astype(str) + ' | ' + daily_anomalies['metric']

# Two column layout
left_col, right_col = st.columns(2)

with left_col:
    st.subheader("Detected Anomalies")
    selected = st.selectbox("Select an anomaly to investigate:", daily_anomalies['key'].unique())

    if selected:
        date_str, metric = selected.split(' | ')
        selected_anomaly = daily_anomalies[daily_anomalies['key'] == selected].iloc[0]
        
        # Format value
        if 'rate' in metric:
            value_display = f"{selected_anomaly['metric_value'] * 100:.2f}%"
        elif 'transactions' in metric:
            value_display = f"{selected_anomaly['metric_value']:,.0f}"
        else:
            value_display = f"{selected_anomaly['metric_value']:.2f}"
        
        col1, col2 = st.columns(2)
        col1.metric(label=metric, value=value_display)
        col2.metric(label="Z-Score", value=f"{selected_anomaly['zscore']:.2f}")
        
        # Drilldown
        st.subheader("Dimension Breakdown")
        drilldown['transaction_date'] = pd.to_datetime(drilldown['transaction_date']).dt.date
        anomaly_date = selected_anomaly['transaction_date']
        
        selected_drilldown = drilldown[
            (drilldown['transaction_date'] == anomaly_date) &
            (drilldown['metric'] == metric)
        ].sort_values('dimension_zscore', ascending=False)
        
        if len(selected_drilldown) > 0:
            st.dataframe(selected_drilldown[['level', 'dimension_value', 'dimension_zscore']])
        else:
            st.write("No dimension-level anomalies detected.")

with right_col:
    st.subheader("AI Hypothesis")
    if selected and len(selected_drilldown) > 0:
        if st.button("Explain with AI"):
            with st.spinner("Generating explanation..."):
                explanation = explain_anomaly(
                    anomaly_date,
                    metric,
                    selected_anomaly['zscore'],
                    selected_drilldown
                )
            st.write(explanation)
    else:
        st.write("Select an anomaly with dimension breakdowns to generate explanation.")