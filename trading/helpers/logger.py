import logging
from logging.handlers import TimedRotatingFileHandler


def get_timed_rotating_log(path: str) -> logging.Logger:
    """
    Returns logger object allowing for logging files with rotation
    according to time paramters
        Parameters:
                path (str): path to the file for logging purpose
        Returns:
                logging.Logger: object used in application for
                                logging purpose
    """
    logging.basicConfig(format="%(asctime)s %(message)s")
    logger = logging.getLogger("Rotating Log")
    logger.setLevel(logging.INFO)

    handler = TimedRotatingFileHandler(path, when="m", interval=1, backupCount=5)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
