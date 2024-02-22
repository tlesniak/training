import argparse
import os
from pyspark.sql import SparkSession
import time

from .helpers import functions as hfn

logger = hfn.get_timed_rotating_log("bitcoin_reporting.log")
logger.info('Starting preparaion of report')


timestr = time.strftime("%Y%m%d-%H%M%S")
report_file_name = 'bitcoin_trading_report' + timestr + '.csv'
required_fields_client=['id', 'first_name', 'last_name', 'email', 'country']
required_fields_financial=['id', 'btc_a', 'cc_t', 'cc_n']

parser = argparse.ArgumentParser(description='Process client data to produce bitcoin trading report.')
parser.add_argument('client_data_directory', type=str,
                    help='path to client data file')
parser.add_argument('financial_data_directory', type=str,
                    help='path to financial data file')
parser.add_argument('--countries', '--names-list', default=["United Kingdom","Netherlands"], nargs='+',
                    help='list of countries for report')


parameters = parser.parse_args()

logger.info(f""" Processing with following parameters:
                 - path to client data file: {parameters.client_data_directory}
                 - path to financial data file: {parameters.financial_data_directory}
                 - countries" {parameters.countries}
            """)

if not os.path.isfile(parameters.client_data_directory):
   logger.error(f'client data path does not exists: {parameters.client_data_directory}')
   exit()
if not os.path.isfile(parameters.financial_data_directory):
   logger.error(f'financial data path does not exists: {parameters.financial_data_directory}')
   exit()

   
spark = SparkSession \
    .builder \
    .master("local[1]") \
    .appName("KommataPari bitcond trading reporting") \
    .getOrCreate()

client_df = spark.read.option("header", True).csv(parameters.client_data_directory)
print(client_df.columns)
client_df.show()
financial_df= spark.read.option("header", True).csv(parameters.financial_data_directory)
print(financial_df.columns)
financial_df.show()
report_df=client_df.join(financial_df,"id")
report_df.show()


logger.info(f""" Succesfull Generation of report in client_data directory
                 file name : {report_file_name}
            """)