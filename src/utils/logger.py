import logging
import sys
import os
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_FILE = LOG_DIR / "pipeline.log"
os.makedirs(LOG_DIR, exist_ok=True)

def get_logger(name : str) -> logging.Logger:
    """
    Create and return a configured logger
    Each module should call this with its own name.
    """
    # Create logger based on module's name
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Config logger if not already exists
    if not logger.handlers:

        # create handler object
        file_handler = logging.FileHandler(LOG_FILE, mode = "a")
        stream_handler = logging.StreamHandler(sys.stdout)

        # create formatter object
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')

        # set format for the handler
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        # set hanlder for the logger
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        
    # return logger
    return logger



