from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    """
    setting up spark session - in this set-up local spark
        Returns:
                spark(SparkSession): object used to access pyspark api
    """
    return (
        SparkSession.builder.master("local[1]")
        .appName("KommataPari bitcon trading reporting")
        .getOrCreate()
    )
