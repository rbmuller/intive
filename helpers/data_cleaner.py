from pyspark.sql import SparkSession, DataFrame
from helpers.spark import create_spark_session
from pyspark.sql.functions import (
    coalesce,
    last_day,
    add_months,
    lit,
    col,
    current_date,
    when,
)


def GetCleanData(spark: SparkSession, sales_data_file_path: str) -> DataFrame:

    # Create session
    spark = create_spark_session(app_name="Sales Data Analysis")
    # load df
    df = spark.read.option("header", "true").csv(sales_data_file_path)
    # Clean data following rules by column type
    # IF a Date is Null it will be assigned to the last day from the previous month.
    # In a real environment we could implement formatting rules by column type or by Business Domain.
    df_cleaned = (
        df.withColumn(
            "store_name",
            when(col("store_name").isNull(), lit("Store_General")).otherwise(
                col("store_name")
            ),
        )
        .withColumn(
            "sale_date",
            coalesce(
                col("sale_date").cast("date"),
                last_day(add_months(current_date(), -1)),
            ),
        )
        .withColumn(
            "sales_amount",
            when(col("sales_amount").isNull(), lit(0)).otherwise(col("sales_amount")),
        )
        .withColumn(
            "product_category",
            when(col("product_category").isNull(), lit("General")).otherwise(
                col("product_category")
            ),
        )
    )
    return df_cleaned
