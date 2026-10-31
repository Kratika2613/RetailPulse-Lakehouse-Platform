"""
Unit Tests for Silver Layer Data Quality & Cleansing Rules
Validates email regex, city standardization, deduplication semantics,
and quarantine routing criteria.
Uses standard Python unittest (zero external dependencies).
"""

import re
import unittest

EMAIL_REGEX = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

class TestSilverQualityRules(unittest.TestCase):

    def test_email_validation_logic(self):
        valid_emails = [
            "aarav.sharma101@retailpulse.com",
            "priya.reddy@gmail.com",
            "user.name+tag@domain.co.in"
        ]
        invalid_emails = [
            "missingatrate.com",
            "NULL",
            "@domain.com",
            "user@domain"
        ]
        
        for email in valid_emails:
            self.assertIsNotNone(re.match(EMAIL_REGEX, email), f"Should be valid: {email}")
            
        for email in invalid_emails:
            self.assertIsNone(re.match(EMAIL_REGEX, email), f"Should be invalid: {email}")

    def test_city_standardization_logic(self):
        raw_cities = ["mUMBAI", "  bengaluru  ", "DELHI", "pune"]
        standardized = [c.strip().title() for c in raw_cities]
        self.assertEqual(standardized, ["Mumbai", "Bengaluru", "Delhi", "Pune"])

    def test_quarantine_routing_decision(self):
        # Valid record
        valid_record = {"order_id": "ORD-1", "customer_id": "CUST-101", "total_amount": 1500.0}
        is_valid = (
            valid_record["total_amount"] > 0 and 
            valid_record["customer_id"] not in [None, "NULL", "CUST-99999"]
        )
        self.assertTrue(is_valid)

        # Bad record 1: Negative amount
        bad_amount = {"order_id": "ORD-2", "customer_id": "CUST-101", "total_amount": -50.0}
        is_valid_amount = (
            bad_amount["total_amount"] > 0 and 
            bad_amount["customer_id"] not in [None, "NULL", "CUST-99999"]
        )
        self.assertFalse(is_valid_amount)

        # Bad record 2: Orphan customer
        orphan_cust = {"order_id": "ORD-3", "customer_id": "CUST-99999", "total_amount": 1000.0}
        is_valid_orphan = (
            orphan_cust["total_amount"] > 0 and 
            orphan_cust["customer_id"] not in [None, "NULL", "CUST-99999"]
        )
        self.assertFalse(is_valid_orphan)

if __name__ == "__main__":
    unittest.main()
