import os
import ccxt
import logging
from dotenv import load_dotenv

logger = logging.getLogger("CCXT_Broker")

class CryptoBroker:
    def __init__(self, exchange_id='binance'):
        """
        Initializes the CCXT Broker connection.
        Safely loads API keys from the .env configuration.
        """
        # Load environment variables securely
        load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../config/.env'))
        
        self.api_key = os.getenv('BINANCE_API_KEY')
        self.secret_key = os.getenv('BINANCE_SECRET_KEY')
        self.exchange_id = exchange_id

        # Instantiate the exchange dynamically
        exchange_class = getattr(ccxt, self.exchange_id)
        
        try:
            self.exchange = exchange_class({
                'apiKey': self.api_key,
                'secret': self.secret_key,
                'enableRateLimit': True, # CCXT handles API rate limits automatically!
            })
            logger.info(f"Successfully initialized {self.exchange_id.upper()} broker via CCXT.")
        except Exception as e:
            logger.error(f"Failed to initialize {self.exchange_id}: {str(e)}")
            self.exchange = None

    def fetch_balance(self):
        """Returns the available balance of the account."""
        if not self.exchange: return None
        try:
            balance = self.exchange.fetch_balance()
            logger.info("Successfully fetched account balance.")
            return balance['total']
        except Exception as e:
            logger.error(f"Error fetching balance: {str(e)}")
            return None

    def place_market_order(self, symbol: str, side: str, amount: float):
        """
        Places a generic market order. 
        'side' must be 'buy' or 'sell'.
        """
        if not self.exchange: return None
        
        logger.info(f"Attempting to {side.upper()} {amount} of {symbol} at Market Price.")
        try:
            order = self.exchange.create_market_order(symbol, side, amount)
            logger.info(f"Order Success! ID: {order['id']}")
            return order
        except Exception as e:
            logger.error(f"Order Creation Failed for {symbol}: {str(e)}")
            return None

    def fetch_historical_data(self, symbol: str, timeframe: str = '5m', limit: int = 100):
        """
        Fetches OHLCV data from the exchange.
        Returns a list of lists: [ [timestamp, open, high, low, close, volume], ... ]
        """
        if not self.exchange: return None
        
        try:
            # Note: CCXT uses string timeframes like '1m', '5m', '1h', '1d'
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            logger.info(f"Fetched {len(ohlcv)} historical candles for {symbol} at {timeframe}.")
            return ohlcv
        except Exception as e:
            logger.error(f"Failed to fetch historical data for {symbol}: {str(e)}")
            return None

