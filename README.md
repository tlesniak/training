Command line program implemented to process client data and financial data 
to produces report of bitcoin trading information in client_data directory
in csv format. it accepts paths to both input files and countries to be processed.

python3 -m KommataPari --help
usage: __main__.py [-h] [--countries COUNTRIES [COUNTRIES ...]] client_data_directory financial_data_directory

Process client data to produce bitcoin trading report.

positional arguments:
  client_data_directory
                        path to client data file
  financial_data_directory
                        path to financial data file

options:
  -h, --help            show this help message and exit
  --countries COUNTRIES [COUNTRIES ...], --names-list COUNTRIES [COUNTRIES ...]
                        list of countries for report