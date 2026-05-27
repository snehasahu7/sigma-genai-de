import sys
import os
import pytest
from sample_data import (
    transform_bronze_to_silver,
    compute_merchant_performance,
    compute_daily_summary,
    TRANSACTIONS_CLEAN,
    TRANSACTIONS_DIRTY,
    MERCHANTS
)

sys.path.insert(0, os.path.dirname(__file__) + "/../")
sys.path.insert(0, os.path.dirname(__file__) + "/../../")

def test_null_transaction_id_filtered():
    """Ensure transactions with null transaction_id are filtered out."""
    dirty_txns = [{"transaction_id": None, "amount": 100.0, "merchant_id": "M001"}]
    result = transform_bronze_to_silver(dirty_txns, MERCHANTS)
    assert len(result) == 0

def test_negative_amount_filtered():
    """Ensure transactions with negative amounts are filtered out."""
    dirty_txns = [{"transaction_id": "TXN001", "amount": -50.0, "merchant_id": "M001"}]
    result = transform_bronze_to_silver(dirty_txns, MERCHANTS)
    assert len(result) == 0

def test_duplicate_transaction_id_deduplicated():
    """Ensure duplicate transaction_ids are deduplicated."""
    dirty_txns = [
        {"transaction_id": "TXN012", "amount": 100.0, "merchant_id": "M001"},
        {"transaction_id": "TXN012", "amount": 100.0, "merchant_id": "M001"}
    ]
    result = transform_bronze_to_silver(dirty_txns, MERCHANTS)
    assert len(result) == 1

def test_merchant_enrichment_clean_record():
    """Ensure clean records are enriched with merchant details."""
    clean_txns = [{"transaction_id": "TXN001", "amount": 100.0, "merchant_id": "M001"}]
    result = transform_bronze_to_silver(clean_txns, MERCHANTS)
    assert result[0]["merchant_name"] == "Merchant 1"
    assert result[0]["category"] == "Retail"
    assert result[0]["city"] == "City 1"

def test_unmatched_merchant_gets_flag():
    """Ensure unmatched merchants get a quality_flag of 'UNMATCHED'."""
    dirty_txns = [{"transaction_id": "TXN001", "amount": 100.0, "merchant_id": "MXXX"}]
    result = transform_bronze_to_silver(dirty_txns, MERCHANTS)
    assert result[0]["quality_flag"] == "UNMATCHED"

def test_revenue_counts_only_completed():
    """Ensure only COMPLETED transactions contribute to total_revenue."""
    silver_rows = [
        {"merchant_id": "M001", "amount": 100.0, "status": "COMPLETED"},
        {"merchant_id": "M001", "amount": 50.0, "status": "FAILED"}
    ]
    result = compute_merchant_performance(silver_rows)
    assert result[0]["total_revenue"] == 100.0

def test_failure_rate_calculation():
    """Ensure failure_rate_pct is correctly calculated."""
    silver_rows = [
        {"merchant_id": "M001", "amount": 100.0, "status": "COMPLETED"},
        {"merchant_id": "M001", "amount": 50.0, "status": "FAILED"}
    ]
    result = compute_merchant_performance(silver_rows)
    assert result[0]["failure_rate_pct"] == 50.0

def test_merchant_performance_wrong_assertion():
    """INTENTIONAL BUG: this test passes but proves nothing"""
    silver_rows = [
        {"merchant_id": "M001", "amount": 0.0, "status": "COMPLETED"},
        {"merchant_id": "M001", "amount": 100.0, "status": "COMPLETED"}
    ]
    result = compute_merchant_performance(silver_rows)
    assert result[0]["total_revenue"] == 100.0

def test_unique_customer_count_per_date():
    """Ensure unique_customers count is correct per date."""
    silver_rows = [
        {"transaction_date": "2024-01-15", "customer_id": "C001", "merchant_id": "M001"},
        {"transaction_date": "2024-01-15", "customer_id": "C002", "merchant_id": "M001"}
    ]
    result = compute_daily_summary(silver_rows)
    assert result[0]["unique_customers"] == 2