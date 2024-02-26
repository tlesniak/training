from pyspark.sql import SparkSession, DataFrame
from pyspark.testing import assertDataFrameEqual
from trading.helpers.databricks_transformations import rename_columns, filter_data

import sys


def test_dataframe_renaming(spark_session: SparkSession, client_data_df: DataFrame):
    print(f"Validation {sys.path}")
    expected_data = [
        (1, "123456-9066070-75757", "debit"),
        (2, "123456-9066070-75758", "credit"),
        (3, "123456-9066070-75759", "debit"),
    ]
    expected_columns = ["client_identifier", "bitcoin_address", "credit_card_type"]
    expected_df = spark_session.createDataFrame(
        data=expected_data, schema=expected_columns
    )
    rename_dict = {
        "id": "client_identifier",
        "btc_a": "bitcoin_address",
        "cc_t": "credit_card_type",
    }
    assertDataFrameEqual(rename_columns(client_data_df, rename_dict), expected_df)


def test_dataframe_filter(spark_session: SparkSession, client_data_df: DataFrame):
    print(f"Validation {sys.path}")
    expected_data = [
        (1, "123456-9066070-75757", "debit"),
        (2, "123456-9066070-75758", "credit"),
    ]
    expected_columns = ["id", "btc_a", "cc_t"]
    expected_df = spark_session.createDataFrame(
        data=expected_data, schema=expected_columns
    )
    assertDataFrameEqual(filter_data(client_data_df, "id in (1, 2)"), expected_df)
