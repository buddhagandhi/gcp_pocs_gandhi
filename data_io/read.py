from pyspark.sql import SparkSession
from sqlalchemy import create_engine
import pandas as pd
import os

# Load DB config from environment or hardcoded for local dev
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": "5432",
    "user": "myuser",
    "password": "mypassword",
    "database": "mydb"
}

# | Key      | Value        |
# | -------- | ------------ |
# | Host     | `127.0.0.1`  |
# | Port     | `5432`       |
# | Database | `mydb`       |
# | User     | `myuser`     |
# | Password | `mypassword` |

# JDBC URL for Spark
JDBC_URL = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

# Properties for Spark JDBC
JDBC_PROPERTIES = {
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "driver": "org.postgresql.Driver"
}


def read_from_postgres(spark: SparkSession, table_name: str):
    """
    Reads a table from PostgreSQL into Spark DataFrame using JDBC.
    """
    print(f"Reading from PostgreSQL table: {table_name}")
    df = spark.read.jdbc(
        url=JDBC_URL,
        table=table_name,
        properties=JDBC_PROPERTIES
    )
    return df


def read_small_table_with_sqlalchemy(table_name: str):
    """
    Reads a small PostgreSQL table using SQLAlchemy + Pandas.
    Returns: Pandas DataFrame
    """
    print(f"Reading small table using SQLAlchemy: {table_name}")
    engine = create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    df = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)
    return df
