import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Kafka config
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "sales_events"

# Master data
PRODUCTS = [
    ("Laptop", "Electronics", 65000),
    ("Mobile", "Electronics", 25000),
    ("Headphones", "Accessories", 3000),
    ("Shoes", "Fashion", 4000),
    ("T-Shirt", "Fashion", 1500),
    ("Watch", "Accessories", 8000),
]

CITIES = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Pune", "Chennai"]

def generate_order():
    product, category, price = random.choice(PRODUCTS)
    quantity = random.randint(1, 3)

    return {
        "order_id": f"ORD-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "product": product,
        "category": category,
        "city": random.choice(CITIES),
        "price": price,
        "quantity": quantity,
        "total_amount": price * quantity,
        "order_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

print("🚀 E-commerce Producer started...")

while True:
    order = generate_order()
    producer.send(topic, value=order)
    print("📦 Sent:", order)
    time.sleep(random.randint(1, 3))  # real-time feel
