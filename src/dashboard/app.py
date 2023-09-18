#!/usr/bin/env python3
"""Streamlit dashboard for real-time e-commerce analytics."""

import os
import time
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd
import streamlit as st
from pymongo import MongoClient
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = "analytics"
MONGO_COLLECTION = "kpis_minute"

# Page config
st.set_page_config(
    page_title="Real-time E-commerce Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


@st.cache_data(ttl=5)  # Cache for 5 seconds
def load_kpi_data(minutes_back: int = 30) -> pd.DataFrame:
    """Load KPI data from MongoDB."""
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Get recent data
        cutoff_time = datetime.utcnow() - timedelta(minutes=minutes_back)
        
        cursor = collection.find(
            {"minute": {"$gte": cutoff_time.isoformat()}},
            sort=[("minute", 1)]
        )
        
        data = list(cursor)
        client.close()
        
        if data:
            df = pd.DataFrame(data)
            df['minute'] = pd.to_datetime(df['minute'])
            return df.sort_values('minute')
        else:
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=[
                'minute', 'sessions', 'page_views', 'add_to_carts', 
                'purchases', 'conversion_rate', 'revenue', 'aov', 'latency_ms_p95'
            ])
            
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()


def format_number(value, suffix=""):
    """Format numbers for display."""
    if pd.isna(value):
        return "N/A"
    
    if value >= 1_000_000:
        return f"{value/1_000_000:.1f}M{suffix}"
    elif value >= 1_000:
        return f"{value/1_000:.1f}K{suffix}"
    else:
        return f"{value:.0f}{suffix}"


def format_currency(value):
    """Format currency values."""
    if pd.isna(value):
        return "$0"
    return f"${value:,.2f}"


def format_percentage(value):
    """Format percentage values."""
    if pd.isna(value):
        return "0%"
    return f"{value*100:.1f}%"


def create_kpi_cards(current_data: Dict, previous_data: Dict):
    """Create KPI cards with current values and changes."""
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        current_sessions = current_data.get('sessions', 0)
        prev_sessions = previous_data.get('sessions', 0)
        change = current_sessions - prev_sessions
        
        st.metric(
            label="Sessions (last min)",
            value=format_number(current_sessions),
            delta=f"{change:+.0f}" if change != 0 else None
        )
    
    with col2:
        current_cr = current_data.get('conversion_rate', 0)
        prev_cr = previous_data.get('conversion_rate', 0)
        change = (current_cr - prev_cr) * 100
        
        st.metric(
            label="Conversion Rate",
            value=format_percentage(current_cr),
            delta=f"{change:+.2f}%" if abs(change) > 0.01 else None
        )
    
    with col3:
        current_revenue = current_data.get('revenue', 0)
        prev_revenue = previous_data.get('revenue', 0)
        change = current_revenue - prev_revenue
        
        st.metric(
            label="Revenue (last min)",
            value=format_currency(current_revenue),
            delta=format_currency(change) if abs(change) > 0.01 else None
        )
    
    with col4:
        current_latency = current_data.get('latency_ms_p95', 0)
        prev_latency = previous_data.get('latency_ms_p95', 0)
        change = current_latency - prev_latency
        
        st.metric(
            label="P95 Latency (ms)",
            value=f"{current_latency:.0f}",
            delta=f"{change:+.0f}" if abs(change) > 1 else None,
            delta_color="inverse"  # Lower latency is better
        )


def create_funnel_chart(current_data: Dict):
    """Create funnel visualization."""
    page_views = current_data.get('page_views', 0)
    add_to_carts = current_data.get('add_to_carts', 0)
    purchases = current_data.get('purchases', 0)
    
    if page_views == 0:
        st.info("No funnel data available")
        return
    
    # Calculate step rates
    view_to_cart = add_to_carts / page_views if page_views > 0 else 0
    cart_to_purchase = purchases / add_to_carts if add_to_carts > 0 else 0
    
    fig = go.Figure(go.Funnel(
        y=["Page Views", "Add to Cart", "Purchases"],
        x=[page_views, add_to_carts, purchases],
        textinfo="value+percent initial+percent previous",
        textposition="inside",
        textfont=dict(color="white", size=14),
        connector=dict(line=dict(color="royalblue", dash="solid", width=2)),
        marker=dict(color=["lightblue", "orange", "lightgreen"])
    ))
    
    fig.update_layout(
        title="Conversion Funnel (Last Minute)",
        font=dict(size=12),
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Show conversion rates
    col1, col2 = st.columns(2)
    with col1:
        st.metric("View → Cart Rate", format_percentage(view_to_cart))
    with col2:
        st.metric("Cart → Purchase Rate", format_percentage(cart_to_purchase))


def create_timeseries_charts(df: pd.DataFrame):
    """Create time series charts for key metrics."""
    
    if df.empty:
        st.info("No time series data available")
        return
    
    # Revenue and Sessions over time
    fig1 = make_subplots(
        rows=2, cols=1,
        subplot_titles=("Revenue Over Time", "Sessions & Conversion Rate"),
        specs=[[{"secondary_y": False}],
               [{"secondary_y": True}]]
    )
    
    # Revenue chart
    fig1.add_trace(
        go.Scatter(
            x=df['minute'], 
            y=df['revenue'],
            mode='lines+markers',
            name='Revenue',
            line=dict(color='green', width=2)
        ),
        row=1, col=1
    )
    
    # Sessions chart
    fig1.add_trace(
        go.Scatter(
            x=df['minute'],
            y=df['sessions'],
            mode='lines+markers', 
            name='Sessions',
            line=dict(color='blue', width=2)
        ),
        row=2, col=1
    )
    
    # Conversion rate on secondary y-axis
    fig1.add_trace(
        go.Scatter(
            x=df['minute'],
            y=df['conversion_rate'] * 100,
            mode='lines+markers',
            name='Conversion Rate (%)',
            line=dict(color='red', width=2, dash='dash'),
            yaxis='y4'
        ),
        row=2, col=1, secondary_y=True
    )
    
    fig1.update_layout(
        height=600,
        title="Key Metrics Timeline",
        showlegend=True
    )
    
    fig1.update_yaxes(title_text="Revenue ($)", row=1, col=1)
    fig1.update_yaxes(title_text="Sessions", row=2, col=1)
    fig1.update_yaxes(title_text="Conversion Rate (%)", row=2, col=1, secondary_y=True)
    
    st.plotly_chart(fig1, use_container_width=True)
    
    # Latency chart
    if 'latency_ms_p95' in df.columns and not df['latency_ms_p95'].isna().all():
        fig2 = px.line(
            df, 
            x='minute', 
            y='latency_ms_p95',
            title='P95 Latency Over Time',
            labels={'latency_ms_p95': 'Latency (ms)', 'minute': 'Time'}
        )
        fig2.update_traces(line_color='purple', line_width=2)
        fig2.update_layout(height=300)
        st.plotly_chart(fig2, use_container_width=True)


def detect_anomalies(df: pd.DataFrame):
    """Simple anomaly detection using z-score."""
    if len(df) < 3:
        return
    
    # Check conversion rate anomalies
    if 'conversion_rate' in df.columns:
        mean_cr = df['conversion_rate'].mean()
        std_cr = df['conversion_rate'].std()
        
        if std_cr > 0:
            latest_cr = df['conversion_rate'].iloc[-1]
            z_score = abs(latest_cr - mean_cr) / std_cr
            
            if z_score > 2:  # More than 2 standard deviations
                if latest_cr > mean_cr:
                    st.success(f"🎉 Conversion rate spike detected! Current: {format_percentage(latest_cr)} (Z-score: {z_score:.1f})")
                else:
                    st.warning(f"⚠️ Conversion rate drop detected! Current: {format_percentage(latest_cr)} (Z-score: {z_score:.1f})")


def main():
    """Main dashboard application."""
    
    st.title("📊 Real-time E-commerce Analytics")
    st.markdown("*Live clickstream analytics with Kafka + Spark Streaming*")
    
    # Sidebar controls
    with st.sidebar:
        st.header("⚙️ Controls")
        
        auto_refresh = st.checkbox("Auto-refresh", value=True)
        refresh_interval = st.slider("Refresh interval (seconds)", 2, 30, 5)
        
        minutes_back = st.slider("Time window (minutes)", 5, 120, 30)
        
        st.markdown("---")
        st.markdown("**Data Source:** MongoDB")
        st.markdown(f"**URI:** `{MONGO_URI}`")
        st.markdown(f"**Collection:** `{MONGO_DB}.{MONGO_COLLECTION}`")
        
        # Manual refresh button
        if st.button("🔄 Refresh Now"):
            st.cache_data.clear()
    
    # Load data
    df = load_kpi_data(minutes_back)
    
    if df.empty:
        st.warning("⚠️ No data available. Make sure the streaming job is running and producing data.")
        st.info("**To start producing data:**\\n\\n1. Run: `docker compose up -d`\\n2. Run: `make topics`\\n3. Run: `make produce` (in another terminal)\\n4. Run: `make stream` (in another terminal)")
        return
    
    # Get current and previous data points
    current_data = df.iloc[-1].to_dict() if len(df) > 0 else {}
    previous_data = df.iloc[-2].to_dict() if len(df) > 1 else {}
    
    # KPI Cards
    st.header("📈 Live KPIs")
    create_kpi_cards(current_data, previous_data)
    
    # Anomaly detection
    detect_anomalies(df)
    
    st.markdown("---")
    
    # Two-column layout for charts
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.header("🔄 Conversion Funnel")
        create_funnel_chart(current_data)
    
    with col2:
        st.header("📋 Data Summary")
        
        if not df.empty:
            st.dataframe(
                df[['minute', 'sessions', 'conversion_rate', 'revenue', 'latency_ms_p95']].tail(10),
                use_container_width=True
            )
        
        # System health
        st.subheader("🏥 System Health")
        if not df.empty:
            latest_latency = current_data.get('latency_ms_p95', 0)
            if latest_latency < 1000:
                st.success(f"✅ Low latency: {latest_latency:.0f}ms")
            elif latest_latency < 3000:
                st.warning(f"⚠️ Medium latency: {latest_latency:.0f}ms")
            else:
                st.error(f"❌ High latency: {latest_latency:.0f}ms")
    
    # Time series charts
    st.header("📊 Time Series")
    create_timeseries_charts(df)
    
    # Footer with last update time
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        st.markdown(f"*Last updated: {datetime.now().strftime('%H:%M:%S')}*")
    
    # Auto-refresh logic
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()

# Updated: 2025-10-04 19:46:28
# Added during commit replay
