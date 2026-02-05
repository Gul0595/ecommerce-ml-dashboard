import streamlit as st
import pandas as pd
import mysql.connector
import plotly.express as px
import time

# -----------------------------
# MySQL connection
# -----------------------------
def get_connection():
    return mysql.connector.connect(
        host="gondola.proxy.rlwy.net",
        user="root",
        password="qXFFWJWhuTIPBdFPgLjHNFXDGUSTwbPC",
        database="railway",
        port=34879
    )

# -----------------------------
# Load data
# -----------------------------
from sqlalchemy import create_engine
import pandas as pd

def get_engine():
    return create_engine(
        "mysql+pymysql://root:qXFFWJWhuTIPBdFPgLjHNFXDGUSTwbPC@gondola.proxy.rlwy.net:34879/railway"
    )

def load_data(query):
    engine = get_engine()
    df = pd.read_sql(query, engine)
    return df

# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="Ecommerce Sales Dashboard", layout="wide")

st.title("📊 Real-Time Ecommerce Sales Dashboard")

# Auto refresh every 5 seconds
refresh_rate = 5
st.caption(f"Auto-refresh every {refresh_rate} seconds")

# -----------------------------
# KPIs
# -----------------------------
kpi_query = """
SELECT
  COALESCE(SUM(net_amount), 0) AS revenue,
  COALESCE(SUM(quantity), 0) AS units,
  COUNT(*) AS orders
FROM sales_events;
"""
kpi = load_data(kpi_query)

revenue = int(kpi["revenue"][0])
units = int(kpi["units"][0])
orders = int(kpi["orders"][0])

col1, col2, col3 = st.columns(3)

col1.metric("💰 Total Revenue", f"₹ {revenue:,.0f}")
col2.metric("📦 Units Sold", f"{units:,}")
col3.metric("🧾 Orders", f"{orders:,}")

st.divider()

# -----------------------------
# Charts
# -----------------------------

# Top cities
city_df = load_data("""
SELECT city, SUM(net_amount) AS revenue
FROM sales_events
GROUP BY city
ORDER BY revenue DESC
""")

fig_city = px.bar(city_df, x="city", y="revenue", title="📍 Revenue by City")
st.plotly_chart(fig_city, use_container_width=True)

# Top products
product_df = load_data("""
SELECT product, SUM(quantity) AS units
FROM sales_events
GROUP BY product
ORDER BY units DESC
""")

fig_product = px.bar(product_df, x="product", y="units", title="📦 Best Selling Products")
st.plotly_chart(fig_product, use_container_width=True)

# Peak hour
hour_df = load_data("""
SELECT HOUR(event_time) AS hour, SUM(net_amount) AS revenue
FROM sales_events
GROUP BY hour
ORDER BY hour
""")

fig_hour = px.line(hour_df, x="hour", y="revenue", title="🕒 Revenue by Hour")
st.plotly_chart(fig_hour, use_container_width=True)

# Best day
day_df = load_data("""
SELECT DAYNAME(event_time) AS day, SUM(net_amount) AS revenue
FROM sales_events
GROUP BY day
ORDER BY revenue DESC
""")

fig_day = px.bar(day_df, x="day", y="revenue", title="📅 Best Sales Day")
st.plotly_chart(fig_day, use_container_width=True)

# -----------------------------
# Auto refresh
# -----------------------------
time.sleep(refresh_rate)
st.rerun()






