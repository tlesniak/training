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
    client_data = [
        (1, "Feliza", "Eusden", "feusden0@ameblo.jp", "France"),
        (2, "Priscilla", "Le Pine", "plepine1@biglobe.ne.jp", "France"),
    ]
    client_data_columns = ["id", "first_name", "last_name", "email", "country"]
    client_data_df = spark_session.createDataFrame(
        data=client_data, schema=client_data_columns
    )
    return client_data_df


@pytest.fixture(scope="session")
def financial_data_df(spark_session):
    financial_data = [
        (1, "123456-9066070-75757", "debit", "4175006996999271"),
        (2, "123456-9066070-75758", "credit", "4175006996999272"),
        (3, "123456-9066070-75759", "debit", "4175006996999273"),
    ]
    financial_data_columns = ["id", "btc_a", "cc_t", "cc_n"]
    financial_data_df = spark_session.createDataFrame(
        data=financial_data, schema=financial_data_columns
    )
    return financial_data_df
