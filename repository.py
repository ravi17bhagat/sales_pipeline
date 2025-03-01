from dagster import Definitions
from assets import orders, products, sales

defs = Definitions(
    assets=[orders, products, sales]
)
