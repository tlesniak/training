import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session(request):
    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("InitializeBitcoinTradingSparkSession")
        .getOrCreate()
    )
    request.addfinalizer(lambda: spark_session.sparkContext.stop())
    return spark_session

@pytest.fixture(scope="session")
def client_data_df(spark_session):
    client_data= [(1, "123456-9066070-75757", "debit"), (2, "123456-9066070-75758", "credit"), (3, "123456-9066070-75759", "debit")]
    client_data_columns = ["id", "btc_a", "cc_t"]
    client_data_df=spark_session.createDataFrame(data=client_data, schema=client_data_columns)
    return client_data_df