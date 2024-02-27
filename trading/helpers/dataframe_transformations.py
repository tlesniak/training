import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col


def rename_columns(df: DataFrame, dict: dict) -> DataFrame:
    """
    renaming columns in dataframe according to definition in given dictionary
        Parameters:
             df(Dataframe): input dataframe to be transformed
             dict(dict): definition of replacement
        Returns:
             Dataframe: tranformed dataframe with new columns names in it
    """
    return df.withColumnsRenamed(dict)


def filter_data(df: DataFrame, cond: str):
    """
    filtering our records from dataframe
        Parameters:
             df(Dataframe): input dataframe to be transformed
             cond(str): condition on which filtering should be performed
        Returns:
             Dataframe - tranformed dataframe without filtered records
    """
    return df.filter(cond)


def report_generation(
    spark: SparkSession,
    client_data_path: str,
    financial_data_path: str,
    countries: list,
    app_logger: logging.Logger,
):
    """
    processing 2 dataframes, client and financial data and producing final bitcoin trading report
        Parameters:
             spark(SparkSession): session object for accessing spark api
             client_data_path(str): path to file with client data
             financial_data_path(str): path to file with financial data
             countries(list): list of countries to be filtered from report
             logger(Logger): logging objecct
    """

    try:
        financial_df = (
            spark.read.option("header", True).csv(financial_data_path).drop("cc_n")
        )
        client_df = spark.read.option("header", True).csv(client_data_path)
        client_with_financial_df = (
            client_df.select("id", "email", "country")
            .filter(col("country").isin(countries))
            .join(financial_df, "id")
            .withColumnsRenamed(
                {
                    "id": "client_identifier",
                    "btc_a": "bitcoin_address",
                    "cc_t": "credit_card_type",
                }
            )
        )
        return client_with_financial_df

    except Exception as e:
        app_logger.error(e)
        raise ValueError("Error in report generation - transformation !!!")
