#!/usr/bin/env python3
"""
RetailPulse Synthetic Data Generator
Generates realistic e-commerce datasets with controlled data quality issues:
1. Customers (CSV - Batch)
2. Products (CSV - Batch)
3. Orders (CSV - Batch)
4. Order Events (JSON - Streaming simulation)

Built using standard Python libraries (zero external dependencies).
"""

import os
import csv
import json
import random
from datetime import datetime, timedelta

# Set deterministic seed for reproducibility
random.seed(42)

# Directory configurations
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BATCH_OUT_DIR = os.path.join(BASE_DIR, "data", "raw", "batch")
STREAMING_OUT_DIR = os.path.join(BASE_DIR, "data", "raw", "streaming")

os.makedirs(BATCH_OUT_DIR, exist_ok=True)
os.makedirs(STREAMING_OUT_DIR, exist_ok=True)

# Sample pools for realistic data
FIRST_NAMES = [
    "Aarav", "Aditi", "Rohan", "Pooja", "Vikram", "Sneha", "Rahul", "Priya",
    "Ananya", "Karan", "Neha", "Amit", "Deepika", "Suresh", "Kavita", "Arjun",
    "Meera", "Varun", "Simran", "Rajesh", "Pooja", "Gaurav", "Divya", "Manish"
]
LAST_NAMES = [
    "Sharma", "Verma", "Patel", "Singh", "Gupta", "Kumar", "Iyer", "Reddy",
    "Joshi", "Chopra", "Mehta", "Nair", "Bose", "Das", "Rao", "Mishra", "Agarwal"
]
CITIES_STATES = [
    ("Mumbai", "Maharashtra"), ("Bengaluru", "Karnataka"), ("Delhi", "Delhi"),
    ("Hyderabad", "Telangana"), ("Pune", "Maharashtra"), ("Chennai", "Tamil Nadu"),
    ("Kolkata", "West Bengal"), ("Ahmedabad", "Gujarat"), ("Jaipur", "Rajasthan"),
    ("Lucknow", "Uttar Pradesh"), ("Chandigarh", "Punjab"), ("Indore", "Madhya Pradesh")
]
PRODUCT_CATALOG = [
    ("Wireless Bluetooth Earbuds", "Electronics", 1999.00),
    ("Noise Cancelling Headphones", "Electronics", 7499.00),
    ("Smart Fitness Watch", "Electronics", 2999.00),
    ("Ultra HD 4K Smart TV 55-inch", "Electronics", 42999.00),
    ("Mechanical Gaming Keyboard", "Electronics", 3499.00),
    ("Cotton Casual T-Shirt", "Apparel", 699.00),
    ("Slim Fit Denim Jeans", "Apparel", 1499.00),
    ("Running Shoes Lightweight", "Apparel", 2299.00),
    ("Waterproof Winter Jacket", "Apparel", 3999.00),
    ("Stainless Steel Water Bottle 1L", "Home & Kitchen", 549.00),
    ("Air Fryer 4.5 Litre Digital", "Home & Kitchen", 5999.00),
    ("Ergonomic Mesh Office Chair", "Home & Kitchen", 8499.00),
    ("Non-Stick Cookware Set 3 Pcs", "Home & Kitchen", 2199.00),
    ("Atomic Habits Paperback", "Books", 499.00),
    ("The Psychology of Money", "Books", 399.00),
    ("Organic Vitamin C Face Serum", "Beauty", 799.00),
    ("Hydrating Sunscreen SPF 50", "Beauty", 499.00),
    ("Anti-Dandruff Herbal Shampoo", "Beauty", 349.00),
    ("Adjustable Dumbbells 20kg Set", "Sports", 4599.00),
    ("Yoga Mat with Carrying Strap", "Sports", 899.00)
]
PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash on Delivery"]
ORDER_STATUSES = ["Delivered", "Delivered", "Delivered", "Shipped", "Pending", "Cancelled", "Returned"]
DEVICE_TYPES = ["Android", "iOS", "Web"]

def generate_customers(num_records=1000):
    """Generates customer records with controlled quality issues (duplicates, malformed emails)."""
    customers = []
    start_date = datetime(2023, 1, 1)
    
    for i in range(1, num_records + 1):
        cust_id = f"CUST-{10000 + i}"
        fn = random.choice(FIRST_NAMES)
        ln = random.choice(LAST_NAMES)
        city, state = random.choice(CITIES_STATES)
        
        # Inconsistent casing bug (10% of records)
        if random.random() < 0.10:
            city = city.upper() if random.random() < 0.5 else city.lower()
            
        signup_dt = (start_date + timedelta(days=random.randint(0, 600))).strftime("%Y-%m-%d")
        
        # Email generation with deliberate quality issues
        rand_val = random.random()
        if rand_val < 0.03:
            email = "NULL"  # Missing email bug
        elif rand_val < 0.06:
            email = f"{fn.lower()}.{ln.lower()}missingatrate.com"  # Invalid format bug
        else:
            email = f"{fn.lower()}.{ln.lower()}{i}@retailpulse.com"
            
        phone = f"+91-{random.randint(70000, 99999)}-{random.randint(10000, 99999)}"
        
        customers.append({
            "customer_id": cust_id,
            "first_name": fn,
            "last_name": ln,
            "email": email,
            "phone": phone,
            "city": city,
            "state": state,
            "country": "India",
            "signup_date": signup_dt
        })
        
    # Inject exact duplicates (3% duplication rate to test deduplication in Silver)
    num_duplicates = int(num_records * 0.03)
    for _ in range(num_duplicates):
        dup_row = random.choice(customers).copy()
        customers.append(dup_row)
        
    random.shuffle(customers)
    
    file_path = os.path.join(BATCH_OUT_DIR, "customers.csv")
    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(customers[0].keys()))
        writer.writeheader()
        writer.writerows(customers)
        
    print(f"Generated {len(customers)} customer records -> {file_path}")
    return customers

def generate_products():
    """Generates product master list with edge cases (negative prices, missing stock)."""
    products = []
    prod_id_counter = 2001
    
    for name, category, price in PRODUCT_CATALOG:
        products.append({
            "product_id": f"PROD-{prod_id_counter}",
            "product_name": name,
            "category": category,
            "price": round(price, 2),
            "stock_quantity": random.randint(10, 500),
            "is_active": "true"
        })
        prod_id_counter += 1
        
    # Inject realistic bad data
    products.append({
        "product_id": f"PROD-{prod_id_counter}",
        "product_name": "Faulty Return Item",
        "category": "Electronics",
        "price": -199.99,  # Bad data: Negative price
        "stock_quantity": -5,  # Bad data: Negative stock
        "is_active": "false"
    })
    
    file_path = os.path.join(BATCH_OUT_DIR, "products.csv")
    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(products[0].keys()))
        writer.writeheader()
        writer.writerows(products)
        
    print(f"Generated {len(products)} product records -> {file_path}")
    return products

def generate_orders(customers, products, num_orders=2500):
    """Generates historical batch orders with anomalies (negative amounts, orphan customers)."""
    orders = []
    base_date = datetime(2024, 1, 1, 9, 0, 0)
    
    cust_ids = [c["customer_id"] for c in customers if c["customer_id"] != "NULL"]
    prod_prices = {p["product_id"]: p["price"] for p in products if p["price"] > 0}
    prod_keys = list(prod_prices.keys())
    
    for i in range(1, num_orders + 1):
        order_id = f"ORD-{50000 + i}"
        
        # 2% orphan customer_id bug (simulates data referential integrity breakdown)
        if random.random() < 0.02:
            cust_id = f"CUST-99999"
        else:
            cust_id = random.choice(cust_ids)
            
        num_items = random.randint(1, 4)
        selected_prods = random.choices(prod_keys, k=num_items)
        calc_amount = sum(prod_prices[pid] for pid in selected_prods)
        
        # Anomaly: 2% negative / zero amount bug
        rand_anomaly = random.random()
        if rand_anomaly < 0.015:
            total_amount = -1.0 * calc_amount
        elif rand_anomaly < 0.03:
            total_amount = 0.0
        else:
            total_amount = round(calc_amount, 2)
            
        order_dt = base_date + timedelta(
            days=random.randint(0, 300),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        status = random.choice(ORDER_STATUSES)
        
        orders.append({
            "order_id": order_id,
            "customer_id": cust_id,
            "order_timestamp": order_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "total_amount": total_amount,
            "order_status": status,
            "payment_method": random.choice(PAYMENT_METHODS),
            "shipping_city": random.choice(CITIES_STATES)[0]
        })
        
    # Inject exact duplicates for orders (4% duplication)
    num_duplicates = int(num_orders * 0.04)
    for _ in range(num_duplicates):
        orders.append(random.choice(orders).copy())
        
    random.shuffle(orders)
    
    file_path = os.path.join(BATCH_OUT_DIR, "orders.csv")
    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(orders[0].keys()))
        writer.writeheader()
        writer.writerows(orders)
        
    print(f"Generated {len(orders)} order records -> {file_path}")
    return orders

def generate_streaming_events(orders, num_events=1500):
    """Generates JSON-lines streaming event stream (Clickstream & Order status updates)."""
    events = []
    event_types = ["ORDER_PLACED", "PAYMENT_CONFIRMED", "ORDER_SHIPPED", "OUT_FOR_DELIVERY", "ORDER_DELIVERED"]
    
    for i in range(1, num_events + 1):
        sample_order = random.choice(orders)
        event_time = datetime.now() - timedelta(minutes=random.randint(1, 1440))
        
        event_payload = {
            "event_id": f"EVT-{100000 + i}",
            "order_id": sample_order["order_id"],
            "customer_id": sample_order["customer_id"],
            "event_type": random.choice(event_types),
            "event_timestamp": event_time.isoformat(),
            "device_type": random.choice(DEVICE_TYPES),
            "ip_address": f"192.168.{random.randint(1, 254)}.{random.randint(1, 254)}",
            "metadata": {
                "app_version": random.choice(["v2.4.1", "v2.5.0", "v2.6.2"]),
                "network_type": random.choice(["4G", "5G", "WiFi"])
            }
        }
        events.append(event_payload)
        
    file_path = os.path.join(STREAMING_OUT_DIR, "order_events.json")
    with open(file_path, mode="w", encoding="utf-8") as f:
        for evt in events:
            f.write(json.dumps(evt) + "\n")
            
    print(f"Generated {len(events)} streaming JSON events -> {file_path}")

if __name__ == "__main__":
    print("==================================================")
    print("🚀 RetailPulse Data Generation Engine Starting...")
    print("==================================================")
    custs = generate_customers(num_records=1000)
    prods = generate_products()
    ords = generate_orders(custs, prods, num_orders=2500)
    generate_streaming_events(ords, num_events=1500)
    print("==================================================")
    print("✅ All Synthetic Datasets Generated Successfully!")
    print("==================================================")

