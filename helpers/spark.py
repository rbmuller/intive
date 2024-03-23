from pyspark.sql import SparkSession


def create_spark_session(app_name="Read Data File", master="local[*]"):
    """
    Create and return a Spark session.

    Parameters:
    app_name: Name of the application (default "Read Data File")
    master: The master URL for the cluster (default "local[*]" to run locally with as many worker threads as logical cores on your machine)

    Returns:
    A SparkSession object
    """
    spark = SparkSession.builder.appName(app_name).master(master).getOrCreate()
    return spark
