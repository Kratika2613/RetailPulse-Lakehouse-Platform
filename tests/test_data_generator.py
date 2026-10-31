"""
Unit Tests for RetailPulse Data Generation Engine
Validates that generated raw files match enterprise schema requirements
and contain controlled data quality test cases.
Uses standard Python unittest (zero external dependencies).
"""

import os
import csv
import json
import unittest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BATCH_DIR = os.path.join(BASE_DIR, "data", "raw", "batch")
STREAMING_DIR = os.path.join(BASE_DIR, "data", "raw", "streaming")

class TestDataGenerator(unittest.TestCase):

    def test_customers_csv_exists_and_populated(self):
        file_path = os.path.join(BATCH_DIR, "customers.csv")
        self.assertTrue(os.path.exists(file_path), "customers.csv must exist")
        
        with open(file_path, "r", encoding="utf-8") as f:
            reader = list(csv.DictReader(f))
            self.assertGreaterEqual(len(reader), 1000, "Should generate at least 1,000 customers")
            required_cols = {"customer_id", "first_name", "last_name", "email", "phone", "city", "state", "country", "signup_date"}
            self.assertTrue(required_cols.issubset(set(reader[0].keys())), "Missing required customer columns")

    def test_products_csv_contains_injected_anomalies(self):
        file_path = os.path.join(BATCH_DIR, "products.csv")
        self.assertTrue(os.path.exists(file_path), "products.csv must exist")
        
        with open(file_path, "r", encoding="utf-8") as f:
            reader = list(csv.DictReader(f))
            self.assertGreaterEqual(len(reader), 20, "Should contain catalog products")
            
            # Verify deliberately injected negative price test anomaly
            negative_price_items = [row for row in reader if float(row["price"]) < 0]
            self.assertGreater(len(negative_price_items), 0, "Deliberate test anomaly (negative price) must exist")

    def test_orders_csv_contains_duplicates_and_anomalies(self):
        file_path = os.path.join(BATCH_DIR, "orders.csv")
        self.assertTrue(os.path.exists(file_path), "orders.csv must exist")
        
        with open(file_path, "r", encoding="utf-8") as f:
            reader = list(csv.DictReader(f))
            self.assertGreaterEqual(len(reader), 2000, "Should contain at least 2000 orders")
            
            # Check for duplicate order_ids to verify deduplication test case
            order_ids = [row["order_id"] for row in reader]
            unique_order_ids = set(order_ids)
            self.assertGreater(len(order_ids), len(unique_order_ids), "Deliberate order duplicates must exist to test silver deduplication")
            
            # Check for negative amounts
            negative_orders = [row for row in reader if float(row["total_amount"]) <= 0]
            self.assertGreater(len(negative_orders), 0, "Deliberate invalid amounts must exist to test quarantine DLQ")

    def test_streaming_json_events_valid_ndjson(self):
        file_path = os.path.join(STREAMING_DIR, "order_events.json")
        self.assertTrue(os.path.exists(file_path), "order_events.json must exist")
        
        count = 0
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                self.assertIn("event_id", payload)
                self.assertIn("order_id", payload)
                self.assertIn("event_type", payload)
                self.assertIn("metadata", payload)
                count += 1
                
        self.assertGreaterEqual(count, 1000, "Should contain at least 1,000 streaming events")

if __name__ == "__main__":
    unittest.main()
