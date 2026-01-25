from kafka import KafkaConsumer
import json
import mysql.connector
from datetime import datetime
import os

# =========================
# 1️⃣ CONNECT TO RAILWAY MYSQL
# =========================
db = mysql.connector.connect(
    host=os.getenv("MYSQLHOST"),
    port=int(os.getenv("MYSQLPORT")),
    user=os.getenv("MYSQLUSER"),
    password=os.getenv("MYSQLPASSWORD"),
    database=os.getenv("MYSQLDATABASE"),
    ssl_disabled=False
)

cursor = db.cursor()
print("✅ Connected to Railway MySQL")

# =========================
# 2️⃣ KAFKA CONSUMER
# =========================
consumer = KafkaConsumer(
    "sales_events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="sales-consumer-group-v4",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("🚀 Kafka Consumer started...")

# =========================
# 3️⃣ CONSUME & INSERT
# =========================
for message in consumer:
    data = message.value

    total_price = data["price"] * data["quantity"]

    event_time = (
        data.get("event_time")
        or data.get("timestamp")
        or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

    sql = """
    INSERT INTO sales
    (order_id, product, city, price, quantity, total_price, event_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    values = (
        data["order_id"],
        data["product"],
        data["city"],
        data["price"],
        data["quantity"],
        total_price,
        event_time
    )

    cursor.execute(sql, values)
    db.commit()

    print("✅ Inserted:", data)
