"""
The entry point of trading report application
"""

from trading.helpers.logger import get_timed_rotating_log
from trading.helpers.session import create_spark_session
from trading.helpers.utils import report_generation, report_csv_save
from trading.helpers.parser import parse_prompt_paramters
import os


def main():

    logger = get_timed_rotating_log("logs/bitcoin_reporting.log")
    spark_session = create_spark_session()

    logger.info("Starting preparaion of report")

    params = parse_prompt_paramters()  # calculate report parameters return 3 arguemtns

    logger.info(
        f""" Processing with following parameters:
                  - path to client data file: {params.client_data_path}
                  - path to financial data file: {params.financial_data_path}
                  - countries" {params.countries}
               """
    )

    repot_df = report_generation(
        spark=spark_session,
        client_data_path=params.client_data_path,
        financial_data_path=params.financial_data_path,
        countries=params.countries,
        app_logger=logger,
    )

    report_csv_save(repot_df, logger)

    logger.info(f"""Succesfull Generation of report in client_data directory""")


if __name__ == "__main__":
    main()
