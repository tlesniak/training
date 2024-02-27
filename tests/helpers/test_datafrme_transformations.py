import sys
import os
import logging
from _pytest.fixtures import FixtureRequest
from pyspark.sql import SparkSession, DataFrame
from pyspark.testing import assertDataFrameEqual
from trading.helpers.dataframe_transformations import (
    rename_columns,
    filter_data,
    report_generation,
)


def test_dataframe_renaming(spark_session: SparkSession, financial_data_df: DataFrame):
    print(f"Validation {sys.path}")
    expected_data = [
        (1, "123456-9066070-75757", "debit", "4175006996999271"),
        (2, "123456-9066070-75758", "credit", "4175006996999272"),
        (3, "123456-9066070-75759", "debit", "4175006996999273"),
    ]
    expected_columns = [
        "client_identifier",
        "bitcoin_address",
        "credit_card_type",
        "cc_n",
    ]
    expected_df = spark_session.createDataFrame(
        data=expected_data, schema=expected_columns
    )
    rename_dict = {
        "id": "client_identifier",
        "btc_a": "bitcoin_address",
        "cc_t": "credit_card_type",
    }
    assertDataFrameEqual(rename_columns(financial_data_df, rename_dict), expected_df)


def test_dataframe_filter(spark_session: SparkSession, client_data_df: DataFrame):
    print(f"Validation {sys.path}")

    expected_data = [
        (1, "Feliza", "Eusden", "feusden0@ameblo.jp", "France"),
        (2, "Priscilla", "Le Pine", "plepine1@biglobe.ne.jp", "France"),
    ]
    expected_columns = ["id", "first_name", "last_name", "email", "country"]
    expected_df = spark_session.createDataFrame(
        data=expected_data, schema=expected_columns
    )
    assertDataFrameEqual(filter_data(client_data_df, "id in (1, 2)"), expected_df)


def test_report_generation(
    request: type[FixtureRequest],
    spark_session: SparkSession,
):
    rootdir = str(request.config.rootdir)
    test_client_data_path = rootdir + "/data/test_client_data.csv"
    test_financial_data_path = rootdir + "/data/test_financial_data.csv"
    test_countries = ["France"]
    logging.basicConfig(format="%(asctime)s %(message)s")
    logger = logging.getLogger("Rotating Log")
    expected_data = [
        (
            "1",
            "feusden0@ameblo.jp",
            "France",
            "1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2",
            "visa-electron",
        ),
        (
            "2",
            "plepine1@biglobe.ne.jp",
            "France",
            "1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T",
            "jcb",
        ),
        (
            "3",
            "jsandes2@reuters.com",
            "France",
            "1CoG9ciLQVQCnia5oXfXPSag4DQ4iYeSpd",
            "diners-club-enroute",
        ),
    ]
    expected_columns = [
        "client_identifier",
        "email",
        "country",
        "bitcoin_address",
        "credit_card_type",
    ]
    expected_df = spark_session.createDataFrame(
        data=expected_data, schema=expected_columns
    )

    actual_df = report_generation(
        spark=spark_session,
        client_data_path=test_client_data_path,
        financial_data_path=test_financial_data_path,
        countries=test_countries,
        app_logger=logger,
    )

    assertDataFrameEqual(actual_df, expected_df)
