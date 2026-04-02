import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Settings:
    # --- Alice Blue ---
    ALICE_BLUE_USERNAME = os.getenv("ALICE_BLUE_USERNAME", "")
    ALICE_BLUE_API_KEY = os.getenv("ALICE_BLUE_API_KEY", "")

    # --- System Settings ---
    ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
    MAX_RISK_PER_TRADE = float(os.getenv("MAX_RISK_PER_TRADE", 0.01))

    @classmethod
    def validate_indian_broker(cls):
        """Validates that all required Indian broker configuration exists."""
        if not cls.ALICE_BLUE_USERNAME or not cls.ALICE_BLUE_API_KEY:
            logging.error("Missing Alice Blue API credentials in .env file.")
            raise ValueError("ALICE_BLUE_USERNAME and ALICE_BLUE_API_KEY must be set.")
        return True

# Initialize a global settings instance
settings = Settings()
