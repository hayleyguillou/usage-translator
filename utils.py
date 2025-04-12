import logging
from constants import DEFAULT_LOG_FILE

def setup_logging(to_file=False, log_file=DEFAULT_LOG_FILE):
    """
    Set up logging configuration.
    """
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

def clean_guid(guid):
    """
    Remove non-alphanumeric characters from a GUID string.
    """
    return ''.join(ch for ch in str(guid) if ch.isalnum()) if guid else ''

def escape_sql_string(s):
    """
    Escape single quotes in a string to protect against SQL insertion.
    """
    return s.replace("'", "''")