import os
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient

load_dotenv()
API_KEY = os.getenv('ALPACA_API_KEY')
SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')

client = TradingClient(API_KEY, SECRET_KEY, paper=True)
try:
    account = client.get_account()
    clock = client.get_clock()
    positions = client.get_all_positions()
    print(f"Status: {'Open' if clock.is_open else 'Closed'}")
    print(f"Portfolio Value: ${account.portfolio_value}")
    print(f"Buying Power: ${account.buying_power}")
    print(f"Number of Positions: {len(positions)}")
    for p in positions:
        print(f" - {p.symbol}: {p.qty} shares @ {p.avg_entry_price}")
except Exception as e:
    print(f"Error: {e}")
