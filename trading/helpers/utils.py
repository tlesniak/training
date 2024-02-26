import glob
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import logging
import time


def validate_directories(list_dir: str, logger: logging.Logger):
    """
    validating if input paths exists (current check only on local machine)
            Parameters:
                list_dir(list): list of directories to be checked
            Returns:
                boolean: True - all paths exists, False - at lease one does not exist
    """
    for dir in list_dir:
        if not os.path.isfile(dir):
            logger.error(f"provided path does not exists: {dir}")
            return False
    return True


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
    timestr = time.strftime("%Y%m%d-%H%M%S")
    project_dir = os.getcwd()
    tmp_file_path = project_dir + "/tmp/bitcoin_trading_report"
    report_file_path = (
        project_dir + "/client_data/bitcoin_trading_report_" + timestr + ".csv"
    )

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
        app_logger.error("Error in report generation !!!")
        app_logger.error(e)


def report_csv_save(
    df: DataFrame,
    app_logger: logging.Logger,
):
    """
    producing single csv with appropriate name
        Parameters:
             df(dataframe): report dataframe to be saved
             logger(Logger): logging objecct
    """
    timestr = time.strftime("%Y%m%d-%H%M%S")
    project_dir = os.getcwd()
    tmp_file_path = project_dir + "/tmp/bitcoin_trading_report"
    report_file_path = (
        project_dir + "/client_data/bitcoin_trading_report_" + timestr + ".csv"
    )

    try:
        df.coalesce(1).write.mode("overwrite").csv(tmp_file_path)
        csv_files = glob.glob("{}/*.{}".format(tmp_file_path, "csv"))
        os.rename(csv_files[0], report_file_path)
        os.rmdir(tmp_file_path)
    except Exception as e:
        app_logger.error("Error saving report !!!")
        app_logger.error(e)
