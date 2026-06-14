from abc import ABC, abstractmethod

class BaseBroker(ABC):
    """
    Abstract Base Class for broker integrations.
    Currently only Alpaca (US equities + options) is supported.
    All implementations must follow this interface.
    """

    @abstractmethod
    def connect(self):
        """Establish connection / session with the broker."""
        pass

    @abstractmethod
    def get_historical_data(self, symbol, interval, start_time, end_time):
        """Fetch historical candle / OHLCV data."""
        pass

    @abstractmethod
    def get_funds(self):
        """Retrieve available trading capital."""
        pass

    @abstractmethod
    def place_order(self, symbol, current_price, quantity, order_type="MARKET", is_buy=True):
        """Format and route an order to the broker's API."""
        pass
