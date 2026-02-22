from utils.logger import get_logger
from db_init import run_ddl
logger = get_logger(__name__)
logger.info("Run main.py")
run_ddl()