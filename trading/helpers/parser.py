import argparse


def parse_prompt_paramters() -> argparse.Namespace:
    """
    Parses command line parameters and returns paramters object
        Returns:
                client_data_path(str): client data path
                financial_data_path(str): financial data path
                countries(list): list of countries to filter in report
    """
    parser = argparse.ArgumentParser(
        description="Process client data to produce bitcoin trading report."
    )
    parser.add_argument("client_data_path", type=str, help="path to client data file")
    parser.add_argument(
        "financial_data_path", type=str, help="path to financial data file"
    )
    parser.add_argument(
        "--countries",
        "--names-list",
        default=["United Kingdom", "Netherlands"],
        nargs="+",
        help="list of countries for report - default **United Kingdom** or **Netherlands**",
    )
    return parser.parse_args()
