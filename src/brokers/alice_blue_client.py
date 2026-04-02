import logging
import datetime
from pyalgotrading.broker.aliceblue import AliceBlue # The generic class or actual alice_blue lib depending on version
from pya3 import * # The official SDK library functions for Alice Blue version 3
from src.brokers.base_broker import BaseBroker
from config.settings import settings

logger = logging.getLogger("AliceBlueClient")

class AliceBlueClient(BaseBroker):
    def __init__(self):
        self.username = settings.ALICE_BLUE_USERNAME
        self.api_key = settings.ALICE_BLUE_API_KEY
        self.alice = None

    def connect(self):
        """Connects and generates a session ID for Alice Blue."""
        logger.info(f"Attempting to connect to Alice Blue for user: {self.username}")
        settings.validate_indian_broker()
        try:
            # Login and get session id
            self.alice = alice_blue(username=self.username, password="", twoFA="", api_secret=self.api_key)
            self.alice.get_session_id()
            logger.info("Successfully connected to Alice Blue.")
        except Exception as e:
            logger.error(f"Failed to connect to Alice Blue: {e}")
            raise

    def get_funds(self):
        """Fetches the available balance for trading."""
        if not self.alice:
            raise ConnectionError("Not connected to Alice Blue. Call connect() first.")
        
        try:
            profile = self.alice.get_balance()
            if profile and isinstance(profile, list) and len(profile) > 0:
                # Based on Alice Blue V3 API structure, find 'cash'
                cash_margin = float(profile[0].get('cash', 0.0))
                return cash_margin
            return 0.0
        except Exception as e:
            logger.error(f"Failed to fetch funds: {e}")
            return 0.0

    def get_historical_data(self, symbol_text, interval, start_time, end_time):
        """
        Fetches historical data from Alice Blue for NSE equity.
        """
        if not self.alice:
            raise ConnectionError("Not connected. Call connect() first.")
            
        try:
             # Example: "RELIANCE" maps to an instrument
             instrument = self.alice.get_instrument_by_symbol("NSE", symbol_text)
             # Interval mapping: 1, 3, 5, 10, 15, 30, 60, D
             data = self.alice.get_historical(instrument, start_time, end_time, interval, False)
             return data
        except Exception as e:
            logger.error(f"Failed fetching historical data for {symbol_text}: {e}")
            return None

    def place_order(self, symbol_text, current_price, quantity, order_type="MARKET", is_buy=True):
        """Places an Intraday (MIS) order on NSE."""
        if not self.alice:
            raise ConnectionError("Not connected. Call connect() first.")
            
        transaction_type = TransactionType.Buy if is_buy else TransactionType.Sell
        
        o_type_enum = OrderType.Market if order_type.upper() == "MARKET" else OrderType.Limit
        
        try:
            instrument = self.alice.get_instrument_by_symbol("NSE", symbol_text)
            
            order_res = self.alice.place_order(
                transaction_type=transaction_type,
                instrument=instrument,
                quantity=quantity,
                order_type=o_type_enum,
                product_type=ProductType.Intraday, # MIS
                price=current_price if order_type.upper() == "LIMIT" else 0.0,
                trigger_price=None,
                stop_loss=None,
                square_off=None,
                trailing_sl=None,
                is_amo=False,
                order_tag='algo_order'
            )
            logger.info(f"Order Placement Response: {order_res}")
            return order_res
        except Exception as e:
            logger.error(f"Order placement failed for {symbol_text}: {e}")
            return None
