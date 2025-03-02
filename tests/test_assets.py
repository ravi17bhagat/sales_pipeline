import pytest
import pandas as pd
import os
from dagster import build_asset_context
from assets import orders, products, sales
from unittest.mock import MagicMock


def test_orders_asset(monkeypatch, mock_orders_data):
    """Test hourly partitioned orders asset."""
    
    # Mock reading CSV files
    monkeypatch.setattr("glob.glob", lambda _: ["dummy.csv"])  # Fake file list
    monkeypatch.setattr("pandas.read_csv", lambda _: mock_orders_data)  # Replace file read with mock data

    # Mock SQLite connection
    mock_conn = MagicMock()
    monkeypatch.setattr("sqlite3.connect", lambda _: mock_conn)

    # Simulate execution for a specific partition (e.g., 2025-02-20-10:00)
    context = build_asset_context(partition_key="2025-02-20-10:00")

    # Run the asset function
    result_df = orders(context)

    # Expected output (filtered by partition hour)
    expected_df = mock_orders_data[mock_orders_data["Order_Timestamp"].dt.strftime("%Y-%m-%d-%H:00") == "2025-02-20-10:00"]

    # Add partition hour column
    expected_df["partition_hour"] = ["2025-02-20-10:00"]

    # Assertions
    assert not result_df.empty, "Resulting DataFrame should not be empty"
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))



def test_products_asset(monkeypatch, mock_products_data):
    """Test products asset."""
    
    # Mock reading CSV file
    monkeypatch.setattr("pandas.read_csv", lambda _: mock_products_data)

    # Mock SQLite connection
    mock_conn = MagicMock()
    monkeypatch.setattr("sqlite3.connect", lambda _: mock_conn)

    # Build a test context
    context = build_asset_context()

    # Run the asset function
    result_df = products(context)

    # Expected DataFrame after renaming
    expected_df = mock_products_data.rename(columns={
        "Product ID": "Product_ID",
        "Sub Category": "Sub_Category",
        "Product Name": "Product_Name"
    })

    # Assert equality
    assert not result_df.empty, "Resulting DataFrame should not be empty"
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))


def test_sales_asset(monkeypatch, mock_orders_data, mock_products_data):
    """Test sales asset."""
    
    # Mock SQLite connection
    mock_conn = MagicMock()
    monkeypatch.setattr("sqlite3.connect", lambda _: mock_conn)

    # Mock `pd.read_sql` to return test data instead of querying DB
    def mock_read_sql(query, conn):
        if "ORDERS" in query:
            return mock_orders_data
        elif "PRODUCTS" in query:
            return mock_products_data
        return pd.DataFrame()  # Empty DataFrame for unexpected queries

    monkeypatch.setattr("pandas.read_sql", mock_read_sql)

    # Build a test context
    context = build_asset_context()

    # Run the asset function
    result_df = sales(context)

    # Expected DataFrame after merging
    expected_df = pd.merge(mock_orders_data, mock_products_data, on="Product_ID", how="inner")
    expected_df = expected_df.filter(["Order_ID","Order_Timestamp","Product_ID","Category","Sub_Category","Product_Name","Quantity","Cost","Discount","Final_Price"])

    # Assert equality
    assert not result_df.empty, "Resulting DataFrame should not be empty"
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))