"""
Centralized Path Configuration
================================
Single source of truth for all filesystem paths used across the pipeline.
All modules should import paths from here instead of computing them inline.
"""
import os

# Project root directory (algo-trading-automation/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Database paths
DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'trading_brain.db')
DUCKDB_PATH = os.path.join(PROJECT_ROOT, 'data', 'trading_brain.duckdb')

# Data directory
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

# Log paths
LOG_DIR = os.path.join(PROJECT_ROOT, 'logs')
PIPELINE_LOG = os.path.join(LOG_DIR, 'pipeline.log')

# Trade log CSV
TRADES_CSV = os.path.join(DATA_DIR, 'pipeline_trades.csv')

# Environment file
ENV_FILE = os.path.join(PROJECT_ROOT, '.env')
