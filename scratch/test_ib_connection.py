import sys
import os
import asyncio

# Setup a default event loop for Python 3.14 asyncio compatibility
try:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
except Exception:
    pass

# Add src and brokers to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src', 'brokers'))

from ib_insync import IB
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("TestIBConnection")

def test_ports():
    ports = [7497, 4002, 7496, 4001]
    for port in ports:
        log.info(f"Attempting to connect to IBKR on port {port}...")
        ib = IB()
        try:
            ib.connect('127.0.0.1', port, clientId=99)
            log.info(f"SUCCESS: Connected on port {port}!")
            log.info(f"Account: {ib.managedAccounts()}")
            log.info(f"Connection state: isConnected={ib.isConnected()}")
            ib.disconnect()
            return port
        except Exception as e:
            log.warning(f"Failed to connect on port {port}: {e}")
    log.error("Could not connect to IBKR on any standard port.")
    return None

if __name__ == '__main__':
    test_ports()
