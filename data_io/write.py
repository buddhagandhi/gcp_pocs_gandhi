from pyspark.sql import DataFrame

def write_to_parquet(df: DataFrame, output_path: str = "output/clean_customers"):
    df.write.mode("overwrite").parquet(output_path)


