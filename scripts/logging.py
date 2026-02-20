import logging
import os
from datetime import datetime

def setup_logging():
    """
    Configure centralized logging for the entire pipeline.
    All modules will log to a single file: logs/pipeline.log
    """
    # Create logs directory
    os.makedirs("logs", exist_ok=True)
    
    # Clear any existing handlers
    root_logger = logging.getLogger()
    if root_logger.handlers:
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)
    
    # Configure root logger
    log_file = "logs/pipeline.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, mode='a')
        ]
    )
    
    logging.info("="*80)
    logging.info(f"Pipeline execution started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("="*80)
