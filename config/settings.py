"""
Global Settings — US Market Configuration
==========================================
Loads API credentials from the root .env file.
All trading is routed exclusively through Alpaca (US equities + options).
"""
import os
import logging
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # --- Alpaca (US Market Broker) ---
    ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
    ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")

    # --- Alpha Vantage (Macro Sentiment) ---
    ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY", "")

    # --- System Settings ---
    ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
    MAX_RISK_PER_TRADE = float(os.getenv("MAX_RISK_PER_TRADE", 0.01))

    @classmethod
    def validate(cls):
        """Validates that all required US broker credentials exist."""
        if not cls.ALPACA_API_KEY or not cls.ALPACA_SECRET_KEY:
            logging.error("Missing Alpaca API credentials in .env file.")
            raise ValueError("ALPACA_API_KEY and ALPACA_SECRET_KEY must be set.")
        return True

settings = Settings()
