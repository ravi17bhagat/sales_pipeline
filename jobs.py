import dagster as dg

# Hourly job to load sales data
load_sales_job = dg.define_asset_job(
    "load_sales_data",
    description='This job materializes orders, products and sales assets every hour',
    selection=["orders", "products", "sales"],  # Specify assets
    partitions_def=dg.HourlyPartitionsDefinition(start_date="2025-02-20-00:00"),
)

# Job schedule
hourly_schedule = dg.ScheduleDefinition(
    job=load_sales_job,
    cron_schedule="0 * * * *",  # Runs every hour at the start of the hour
)
