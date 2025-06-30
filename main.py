"""
Main module for running the ETL pipeline:
- Reads data from PostgreSQL
- Cleans customer data
- Joins order_items with products
- Writes output to Parquet
"""

import os
import logging
from pyspark.sql import SparkSession
from data_io.read import read_from_postgres, read_small_table_with_sqlalchemy
from data_io.write import write_to_parquet
from transformations.clean_data import clean_customer_data

# Set JAVA_HOME before starting Spark
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"

# PostgreSQL driver path
POSTGRES_JAR = os.path.expanduser("~/Downloads/postgresql-42.7.7.jar")

# --------------------
# Setup Python Logging
# --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Loading JDBC driver from:\n%s", POSTGRES_JAR)

    spark = (
        SparkSession.builder.appName("PostgresToSpark")
        .config("spark.jars", POSTGRES_JAR)
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1. Read and clean customers
    logger.info("Reading and cleaning customer data")
    df_customers = read_from_postgres(spark, table_name="public.customers")
    df_transformed = clean_customer_data(df_customers)
    df_transformed.show()
    write_to_parquet(df_transformed)

    # 2. Join order_items with products (useful data engineering pattern)
    logger.info("Reading order_items and products for join")
    df_order_items = read_from_postgres(spark, table_name="public.order_items")

    product_dim_pd = read_small_table_with_sqlalchemy("public.products")
    logger.info("Product dimension schema:\n%s", product_dim_pd.dtypes)
    logger.info("First 5 product rows:\n%s", product_dim_pd.head(5))


    product_dim_pd["product_id"] = product_dim_pd["product_id"].astype(int)
    product_dim_pd["name"] = product_dim_pd["name"].astype(str)
    product_dim_pd["category"] = product_dim_pd["category"].astype(str)
    product_dim_pd["price"] = product_dim_pd["price"].astype(float)
    product_dim_pd["supplier_id"] = product_dim_pd["supplier_id"].fillna(0).astype(int)

    product_dim_spark = spark.createDataFrame(product_dim_pd)

    logger.info("Joining order_items with products on product_id")
    enriched_orders = df_order_items.join(product_dim_spark, on="product_id", how="left")
    enriched_orders.show(5)

    if enriched_orders.count() > 0:
        logger.info("Writing enriched_orders to Parquet")
        write_to_parquet(enriched_orders, "output/enriched_orders")
    else:
        logger.warning("No data to write. Join might have failed.")

    spark.stop()
    logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
