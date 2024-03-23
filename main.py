import json
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.window import Window
from helpers.data_cleaner import GetCleanData, create_spark_session

app = FastAPI()

# Initialize Spark session and ingest the data
try:
    spark = create_spark_session()
    df = GetCleanData(
        spark, "data/sales_data.csv"
    ).cache()  # Cache the DataFrame for reuse
except Exception as e:
    app.logger.error(f"Failed to load sales data: {e}")
    raise HTTPException(
        status_code=500, detail="Failed to initialize Spark session or ingest data"
    )


# Response models
class StoreSales(BaseModel):
    store_name: str
    total_revenue: float


class TotalProductCount(BaseModel):
    store_name: str
    qty_products_sold: int


class TopSellingProduct(BaseModel):
    store_name: str
    product_category: str
    total_sales: float


class AvgSalesPriceResponse(BaseModel):
    product_category: str
    average_sales_price: float


# Helper function to apply filters
def apply_filters(
    df: DataFrame, store_name: Optional[str], product_category: Optional[str]
) -> DataFrame:
    if store_name:
        df = df.filter(F.col("store_name") == store_name)
    if product_category:
        df = df.filter(F.col("product_category") == product_category)
    return df


# Helper function to check if DataFrame is empty
def check_empty_data(df: DataFrame):
    if df.rdd.isEmpty():
        raise HTTPException(
            status_code=404, detail="No data found with specified filters."
        )


@app.get("/")
async def root() -> Dict[str, Any]:
    return {
        "header": "Welcome to the Intive Sales API",
        "endpoints": {
            "/total_revenue_per_store": {"parameters": ["store_name"]},
            "/total_products_per_store": {
                "parameters": ["store_name", "product_category"]
            },
            "/avg_sales_price_per_category": {"parameters": ["product_category"]},
            "/top_selling_products": {"parameters": ["store_name", "product_category"]},
        },
    }


@app.get("/total_revenue_per_store", response_model=List[StoreSales])
async def total_sales_per_store(store_name: Optional[str] = Query(None)):
    try:
        filtered_df = apply_filters(df, store_name, None)
        check_empty_data(filtered_df)
        result_df = filtered_df.groupBy("store_name").agg(
            F.sum("sales_amount").alias("total_revenue")
        )
        return result_df.toJSON().map(lambda x: json.loads(x)).collect()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/total_products_per_store", response_model=List[TotalProductCount])
async def total_products_per_store(
    store_name: Optional[str] = Query(None),
    product_category: Optional[str] = Query(None),
):
    try:
        filtered_df = apply_filters(df, store_name, product_category)
        check_empty_data(filtered_df)
        result_df = (
            filtered_df.groupBy("store_name")
            .count()
            .withColumnRenamed("count", "qty_products_sold")
        )
        return result_df.toJSON().map(lambda x: json.loads(x)).collect()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/avg_sales_price_per_category", response_model=List[AvgSalesPriceResponse])
async def avg_sales_price_per_category(product_category: Optional[str] = Query(None)):
    try:
        filtered_df = apply_filters(df, None, product_category)
        check_empty_data(filtered_df)
        result_df = filtered_df.groupBy("product_category").agg(
            F.avg("sales_amount").alias("average_sales_price")
        )
        return result_df.toJSON().map(lambda x: json.loads(x)).collect()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/top_selling_products", response_model=List[TopSellingProduct])
async def top_selling_products(
    store_name: Optional[str] = Query(None),
    product_category: Optional[str] = Query(None),
):
    try:
        filtered_df = apply_filters(df, store_name, product_category)
        check_empty_data(filtered_df)
        window_spec = Window.partitionBy("store_name").orderBy(F.desc("total_sales"))
        result_df = (
            filtered_df.groupBy("store_name", "product_category")
            .agg(F.sum("sales_amount").alias("total_sales"))
            .withColumn("rank", F.rank().over(window_spec))
            .filter(F.col("rank") == 1)
            .drop("rank")
        )
        return result_df.toJSON().map(lambda x: json.loads(x)).collect()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
