import time
import schedule
import pytz
from datetime import datetime, date, timedelta
import logging
import sys
import os

# Ensure the root project directory is in PYTHONPATH if run directly
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.brokers.alice_blue_client import AliceBlueClient
from src.data.historical import format_alice_blue_historical
from src.strategies.emas_crossover import EMACrossoverStrategy
from src.execution.risk_manager import RiskManager
from src.execution.order_router import OrderRouter

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Orchestrator")

# Timezones
IST = pytz.timezone('Asia/Kolkata')

class TradingSystem:
    def __init__(self):
        self.broker = AliceBlueClient()
        self.strategy = EMACrossoverStrategy(symbol="RELIANCE") # Example equity
        self.risk_manager = None # Will instantiate after getting funds
        self.order_router = None
        self.is_connected = False

    def initialize(self):
        try:
            self.broker.connect()
            self.is_connected = True
            
            # Initialize capital and risk systems
            capital = self.broker.get_funds()
            logger.info(f"Available Trading Capital: {capital}")
            
            self.risk_manager = RiskManager(total_capital=capital)
            self.order_router = OrderRouter(broker=self.broker, risk_manager=self.risk_manager)
            
            logger.info("Trading System Initialized Successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize trading system: {e}")
            self.is_connected = False

    def is_market_open(self):
        now_ist = datetime.now(IST)
        
        # 1. Check if it's a weekend
        if now_ist.weekday() >= 5: # 5=Sat, 6=Sun
            return False
            
        # 2. Check time (09:15 to 15:20) - Ending slightly before close to avoid auto-square off penalties
        market_open = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
        market_close = now_ist.replace(hour=15, minute=20, second=0, microsecond=0)
        
        return market_open <= now_ist <= market_close

    def execute_indian_market_cycle(self):
        """
        The main cyclical function that polls data, checks strategy, and trades.
        Intended to run every few minutes.
        """
        if not self.is_market_open():
             now_ist = datetime.now(IST)
             logger.info(f"Market Closed at {now_ist.strftime('%Y-%m-%d %H:%M:%S')}. Waiting...")
             return
             
        if not self.is_connected:
             logger.warning("Broker disconnected. Attempting to re-initialize...")
             self.initialize()
             if not self.is_connected: return
             
        logger.info("Executing Algorithm Cycle...")
        
        try:
            # 1. Fetch Historical Data (Last 5 days to ensure enough EMA data)
            end_time = datetime.now(IST)
            start_time = end_time - timedelta(days=5)
            
            raw_data = self.broker.get_historical_data(
                symbol_text=self.strategy.symbol,
                interval="5", # 5 minute intervals
                start_time=start_time,
                end_time=end_time
            )
            
            # 2. Process Data
            df = format_alice_blue_historical(raw_data)
            if df.empty:
                 logger.warning("Empty dataframe received. Skipping cycle.")
                 return
                 
            current_price = df.iloc[-1]['close'] if 'close' in df.columns else 0
            
            # 3. Analyze Strategy
            signal = self.strategy.analyze(df)
            
            # 4. Route Order
            if signal != 0 and current_price > 0:
                 self.order_router.process_signal(self.strategy.symbol, signal, current_price)
                 
        except Exception as e:
            logger.error(f"Error during market cycle: {e}")


def main():
    logger.info("Starting Global Trading Orchestrator for Indian Market...")
    
    bot = TradingSystem()
    bot.initialize()
    
    # Run cycle every 5 minutes
    schedule.every(5).minutes.do(bot.execute_indian_market_cycle)
    
    # Execute one immediately on start
    bot.execute_indian_market_cycle()
    
    logger.info("Scheduler Active. Bot is looping and monitoring...")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1) # Sleep prevents 100% CPU usage
    except KeyboardInterrupt:
        logger.info("Orchestrator stopped manually.")

if __name__ == "__main__":
    main()
