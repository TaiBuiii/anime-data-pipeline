import logging
import os

LOG_FILE = "logs/pipeline.log"
os.makedirs("logs", exist_ok= True)

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
        handler = logging.FileHandler(LOG_FILE, mode = "w")
        # create formatter object
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # set format for the handler
        handler.setFormatter(formatter)
        # set hanlder for the logger
        logger.addHandler(handler)
    # return logger
    return logger



