import logging
from constants import DEFAULT_LOG_FILE

def setup_logging(to_file=False, log_file=DEFAULT_LOG_FILE):
    log_format = "%(asctime)s [%(levelname)s] %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    if to_file:
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            datefmt=date_format,
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        logging.info(f"Logging to file: {log_file}")
    else:
        logging.basicConfig(level=logging.INFO, format=log_format, datefmt=date_format)