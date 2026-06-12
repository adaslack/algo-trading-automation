"""
Shared Logger for the Pipeline
===============================
All Wheels import this module to write to a single unified log file.
The log file is at: <project_root>/pipeline.log
You can `tail -f pipeline.log` or open it in VSCode to watch live action.
"""
import logging
import os
import sys

# Ensure stdout and stderr support UTF-8 on Windows console
for stream in (sys.stdout, sys.stderr):
    if hasattr(stream, 'reconfigure'):
        try:
            stream.reconfigure(encoding='utf-8', errors='backslashreplace')
        except Exception:
            pass


LOG_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'logs', 'pipeline.log')
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

def get_logger(wheel_name):
    """
    Returns a logger that writes to both the console AND pipeline.log.
    Each log line is prefixed with timestamp + wheel name.
    
    Usage:
        from logger import get_logger
        log = get_logger("Wheel 1")
        log.info("Fetching data for AAPL...")
    """
    logger = logging.getLogger(wheel_name)
    
    # Avoid adding duplicate handlers if called multiple times
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.DEBUG)
    
    # Format: 2026-05-19 09:30:15 | Wheel 1 | INFO | Fetching data for AAPL...
    formatter = logging.Formatter(
        '%(asctime)s | %(name)-12s | %(levelname)-5s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler (append mode so the log grows throughout the day)
    fh = logging.FileHandler(LOG_FILE, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    # Console handler (so you still see output in the terminal too)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    return logger

