import pandas as pd
import logging
from src.strategies.base_strategy import BaseStrategy

logger = logging.getLogger("Strategy")

class EMACrossoverStrategy(BaseStrategy):
    """
    A simple Exponential Moving Average Crossover Strategy.
    Generates a BUY signal when fast EMA crosses above slow EMA.
    Generates a SELL signal when fast EMA crosses below slow EMA.
    """
    
    def __init__(self, symbol, fast_period=9, slow_period=21):
        super().__init__(name="EMA Crossover", symbol=symbol)
        self.fast_period = fast_period
        self.slow_period = slow_period
        
    def analyze(self, df: pd.DataFrame) -> int:
        """
        Analyzes the DataFrame to generate a signal.
        Returns:
            1 if BUY
           -1 if SELL
            0 if HOLD
        """
        if df.empty or len(df) < self.slow_period:
            logger.info("Not enough data to calculate EMA")
            return 0
            
        try:
            # Calculate EMAs
            df['fast_ema'] = df['close'].ewm(span=self.fast_period, adjust=False).mean()
            df['slow_ema'] = df['close'].ewm(span=self.slow_period, adjust=False).mean()
            
            # Get the last two rows to check for a crossover
            last_row = df.iloc[-1]
            prev_row = df.iloc[-2]
            
            # Bullish Crossover (BUY)
            if prev_row['fast_ema'] <= prev_row['slow_ema'] and last_row['fast_ema'] > last_row['slow_ema']:
                logger.info(f"{self.symbol}: BUY SIGNAL - Fast EMA ({self.fast_period}) crossed above Slow EMA ({self.slow_period})")
                return 1
                
            # Bearish Crossover (SELL)
            elif prev_row['fast_ema'] >= prev_row['slow_ema'] and last_row['fast_ema'] < last_row['slow_ema']:
                logger.info(f"{self.symbol}: SELL SIGNAL - Fast EMA ({self.fast_period}) crossed below Slow EMA ({self.slow_period})")
                return -1
                
            # No crossover (HOLD)
            else:
                return 0
                
        except Exception as e:
            logger.error(f"Error executing Strategy analysis for {self.symbol}: {e}")
            return 0
