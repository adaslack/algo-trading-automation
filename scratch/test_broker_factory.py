import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src', 'brokers'))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src', 'pipeline'))

import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("TestBrokerFactory")

from broker_factory import get_broker

def test_broker():
    try:
        broker = get_broker(auto_connect=True)
        log.info(f"Successfully retrieved broker instance: {broker.name}")
        log.info(f"Connection state: is_connected={broker.is_connected()}")
        
        # Test basic info
        account = broker.get_account()
        log.info(f"Account Portfolio Value: {account.portfolio_value}")
        log.info(f"Account Buying Power: {account.buying_power}")
        log.info(f"Account Cash: {account.cash}")
        
        # Test positions
        positions = broker.get_positions()
        log.info(f"Positions: {positions}")
        
        broker.disconnect()
        log.info("Test completed successfully.")
    except Exception as e:
        log.error(f"Failed to test broker connection: {e}")

if __name__ == '__main__':
    test_broker()
