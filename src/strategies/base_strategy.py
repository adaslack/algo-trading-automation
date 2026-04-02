from abc import ABC, abstractmethod

class BaseStrategy(ABC):
    """
    Abstract base class for all trading strategies.
    Ensures they all implement a common interface.
    """
    
    def __init__(self, name="Undefined Strategy", symbol=""):
        self.name = name
        self.symbol = symbol
        
    @abstractmethod
    def analyze(self, df):
        """
        Process the dataframe (historical data) and return signals.
        Returns: 1 for BUY, -1 for SELL, 0 for HOLD.
        """
        pass
