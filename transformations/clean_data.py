from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper

def clean_customer_data(df: DataFrame) -> DataFrame:
    return df.filter(col("age").isNotNull()) \
             .withColumn("name_upper", upper(col("name")))
