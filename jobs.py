import dagster as dg

# Define a job that materializes all hourly-partitioned assets
load_sales_job = dg.define_asset_job(
    "load_sales_data",
    description='This job materializes orders, products and sales assets every hour',
    selection=["orders", "products", "sales"],  # Specify assets
    partitions_def=dg.HourlyPartitionsDefinition(start_date="2025-02-20-00:00"),
)

# Run job every hour
hourly_schedule = dg.ScheduleDefinition(
    job=load_sales_job,
    cron_schedule="0 * * * *",  # Runs every hour at the start of the hour
)
