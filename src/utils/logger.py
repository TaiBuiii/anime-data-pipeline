import logging
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
        handler = logging.FileHandler(LOG_FILE, mode = "a")
        # create formatter object
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
        # set format for the handler
        handler.setFormatter(formatter)
        # set hanlder for the logger
        logger.addHandler(handler)
    # return logger
    return logger



