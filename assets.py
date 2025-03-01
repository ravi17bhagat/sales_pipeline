import sqlite3
import pandas as pd
import glob
import os
import dagster as dg

hourly_partitions = dg.HourlyPartitionsDefinition(start_date="2025-02-20-00:00")

# Orders Asset
@dg.asset(
    description='This is an Orders asset containing hourly partitioned Orders data',
    partitions_def=hourly_partitions,
    metadata={
        "dagster/column_schema": dg.TableSchema(
            columns=[
                dg.TableColumn("Order_ID","string",description="Unique ID for order",),
                dg.TableColumn("Order_Timestamp","datetime",description="Time when order was placed",),
                dg.TableColumn("Customer_Name","string",description="Unique ID for a order",),
                dg.TableColumn("Product_ID","int",description="ID of the Product",),
                dg.TableColumn("Quantity","int",description="Number of units ordered",),
                dg.TableColumn("Cost","float",description="Cost per unit for product",),
                dg.TableColumn("Discount","float",description="Discount per unit for product",),
                dg.TableColumn("Final_Price","float",description="Final price for the order",),
            ]
        )
    },
)
def orders(context):
    orders_path = './data/orders/'

    # Read all orders files
    all_files = glob.glob(os.path.join(orders_path, "*.csv"))
    orders_df = [pd.read_csv(f) for f in all_files]
    df = pd.concat(orders_df, ignore_index=True) 

    # Rename columns
    df.rename(columns={'Order ID':'Order_ID', 'Order Timestamp':'Order_Timestamp', 'Customer Name':'Customer_Name', 'Product ID':'Product_ID','Final Price':'Final_Price'}, inplace=True)

    # Convert to required datatype
    dtype_schema = {
        "Order_ID": "str",
        "Order_Timestamp": "datetime64",
        "Customer_Name": "str",
        "Product_ID": "int",
        "Quantity": "int",
        "Cost": "float",
        "Discount": "float",
        "Final_Price": "float"
    }

    for col, dtype in dtype_schema.items():
        if dtype == 'datetime64':
            df[col] = pd.to_datetime(df[col])
        else:
            df[col] = df[col].astype(dtype)

    # Filter data for required partition
    partition_hour = context.partition_key 
    df["partition_hour"] = df['Order_Timestamp'].dt.strftime("%Y-%m-%d-%H:00")
    df = df[df['partition_hour']==partition_hour]
    context.log.info(f"Sample data: {df.head()}")

    # Insert to sqlite Database
    with sqlite3.connect("db.sqlite3") as conn:
        df.to_sql("orders", conn, if_exists="append", index=False)

    context.log.info(f"Inserted {len(df)} rows for partition {partition_hour}")
    return df

# Products Asset
@dg.asset(
    description='This is Products asset containing Product information',
    metadata={
        "dagster/column_schema": dg.TableSchema(
            columns=[
                dg.TableColumn("Product_ID","int",description="ID of the Product",),
                dg.TableColumn("Category","string",description="Broad Category of the product",),
                dg.TableColumn("Sub_Category","string",description="Specific category of the product",),
                dg.TableColumn("Product_Name","string",description="Name of the product",),
                dg.TableColumn("Description","string",description="Description of the product",),
                dg.TableColumn("Price","float",description="Cost per unit for product",),
                dg.TableColumn("Rating","float",description="Average Rating of the product",),
            ]
        )
    },
)
def products(context):
    products_path = './data/products/'

    # Read Products data
    df = pd.read_csv(f"{products_path}/products.csv")
    context.log.info(f"Sample data: {df.head()}")

    # Rename columns
    df.rename(columns={'Product ID':'Product_ID','Sub Category':'Sub_Category','Product Name':'Product_Name'}, inplace=True)

    # Convert to required datatype
    dtype_schema = {
        "Product_ID": "int",
        "Category": "str",
        "Sub_Category": "str",
        "Product_Name": "str",
        "Description": "str",
        "Price": "float",
        "Rating": "float"
    }

    for col, dtype in dtype_schema.items():
        df[col] = df[col].astype(dtype)

    # Insert to sqlite Database
    with sqlite3.connect("db.sqlite3") as conn:
        df.to_sql("products", conn, if_exists="append", index=False)

    context.log.info(f"Inserted {len(df)} rows")
    return df

# Sales Asset
@dg.asset(
    description='This is a Sales asset containing Orders and Products information',
    deps=["orders", "products"],
    metadata={
        "dagster/column_schema": dg.TableSchema(
            columns=[
                dg.TableColumn("Order_ID","string",description="Unique ID for order",),
                dg.TableColumn("Order_Timestamp","datetime",description="Time when order was placed",),
                dg.TableColumn("Product_ID","int",description="ID of the Product",),
                dg.TableColumn("Category","string",description="Broad Category of the product",),
                dg.TableColumn("Sub_Category","string",description="Specific category of the product",),
                dg.TableColumn("Product_Name","string",description="Name of the product",),
                dg.TableColumn("Quantity","int",description="Number of units ordered",),
                dg.TableColumn("Cost","float",description="Cost per unit for product",),
                dg.TableColumn("Discount","float",description="Discount per unit for product",),
                dg.TableColumn("Final_Price","float",description="Final price for the order",),
            ]
        )
    },
)
def sales(context):
    with sqlite3.connect("db.sqlite3") as conn:
        # Read orders and products data from db
        orders_df = pd.read_sql("SELECT * FROM ORDERS;", conn)
        products_df = pd.read_sql("SELECT * FROM PRODUCTS;", conn)

        # Join the 2 dataframes on Product_ID
        sales_df = pd.merge(orders_df, products_df, on="Product_ID", how="inner")

        # Filter the required columns
        sales_df = sales_df.filter(["Order_ID","Order_Timestamp","Product_ID","Category","Sub_Category","Product_Name","Quantity","Cost","Discount","Final_Price"])
        context.log.info(f"Sample data: {sales_df.head()}")

        # Insert to sqlite Database
        sales_df.to_sql("sales", conn, if_exists="append", index=False)
    return sales_df
