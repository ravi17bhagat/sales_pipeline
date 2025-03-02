import pytest
import pandas as pd

@pytest.fixture
def mock_orders_data():
    """Mock orders data as a Pandas DataFrame."""
    return pd.DataFrame({
        "Order_ID": ["O1", "O2", "O3"],
        "Order_Timestamp": pd.to_datetime(["2025-02-20 10:30:00", "2025-02-20 11:15:00", "2025-02-20 09:45:00"]),
        "Customer_Name": ["Alice", "Bob", "Charlie"],
        "Product_ID": [101, 102, 103],
        "Quantity": [1, 2, 3],
        "Cost": [10.0, 20.0, 30.0],
        "Discount": [1.0, 2.0, 3.0],
        "Final_Price": [9.0, 18.0, 27.0]
    })


@pytest.fixture
def mock_products_data():
    """Mock product data as a Pandas DataFrame."""
    return pd.DataFrame({
        "Product_ID": [101, 102, 103],  # Before renaming
        "Category": ["Electronics", "Clothing", "Books"],
        "Sub Category": ["Mobile", "Shirt", "Novel"],  # Before renaming
        "Product Name": ["iPhone", "T-shirt", "Harry Potter"],  # Before renaming
        "Description": ["Smartphone", "Cotton T-shirt", "Fantasy Book"],
        "Price": [999.99, 19.99, 10.99],
        "Rating": [4.5, 4.0, 4.8]
    })