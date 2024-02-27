import glob
import os
import logging
import time
from pyspark.sql import DataFrame


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
    tmp_file_path = os.path.join(project_dir, "tmp_bitcoin_trading_report")
    report_file_path = os.path.join(
        project_dir, "client_data", "bitcoin_trading_report_" + timestr + ".csv"
    )

    try:
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_file_path)
        csv_files = glob.glob("{}/*.{}".format(tmp_file_path, "csv"))
        os.rename(csv_files[0], report_file_path)
        # os.rmdir(tmp_file_path)
    except Exception as e:
        app_logger.error(e)
        raise ValueError("Error in report save !!!")
