from dagster import Definitions
from assets import orders, products, sales
from jobs import load_sales_job, hourly_schedule

defs = Definitions(
    assets=[orders, products, sales],
    jobs=[load_sales_job],
    schedules=[hourly_schedule],
)
