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
        host="localhost",
        user="root",
        password="",       # empty if using XAMPP default
        database="kafka_db",
        port= 3307
    )

# -----------------------------
# Load data
# -----------------------------
def load_data(query):
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
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
  COALESCE(SUM(price * quantity), 0) AS revenue,
  COALESCE(SUM(quantity), 0) AS units,
  COUNT(*) AS orders
FROM sales;
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
SELECT city, SUM(total_price) AS revenue
FROM sales
GROUP BY city
ORDER BY revenue DESC
""")

fig_city = px.bar(city_df, x="city", y="revenue", title="📍 Revenue by City")
st.plotly_chart(fig_city, use_container_width=True)

# Top products
product_df = load_data("""
SELECT product, SUM(quantity) AS units
FROM sales
GROUP BY product
ORDER BY units DESC
""")

fig_product = px.bar(product_df, x="product", y="units", title="📦 Best Selling Products")
st.plotly_chart(fig_product, use_container_width=True)

# Peak hour
hour_df = load_data("""
SELECT HOUR(event_time) AS hour, SUM(total_price) AS revenue
FROM sales
GROUP BY hour
ORDER BY hour
""")

fig_hour = px.line(hour_df, x="hour", y="revenue", title="🕒 Revenue by Hour")
st.plotly_chart(fig_hour, use_container_width=True)

# Best day
day_df = load_data("""
SELECT DAYNAME(event_time) AS day, SUM(total_price) AS revenue
FROM sales
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
