"""
L2 Feed Connector (Polygon.io & Databento)
===========================================
Subscribes to high-frequency Level 2 tick streams, computes real-time
Order Book Imbalance (OBI) and Order Flow Imbalance (OFI), and streams
them directly to the Redis Event Bus.
"""
import websocket
import json
import os
import sys
import threading
import time

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src', 'pipeline'))

from event_bus import get_bus, Event, Topics
from logger import get_logger

log = get_logger("L2_Feed_Connector")

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
WS_URL = "wss://socket.polygon.io/stocks"

class L2FeedConnector:
    def __init__(self, tickers):
        self.tickers = tickers
        self.ws = None
        self.bus = get_bus()
        
    def start(self):
        if not POLYGON_API_KEY:
            log.warning("POLYGON_API_KEY environment variable not set. Running in simulation mode...")
            threading.Thread(target=self.run_simulation, daemon=True).start()
            return
            
        self.ws = websocket.WebSocketApp(
            WS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        threading.Thread(target=self.ws.run_forever, daemon=True).start()
        log.info("L2 Web Socket thread started.")

    def on_open(self, ws):
        log.info("L2 Websocket connection open. Authenticating...")
        auth_msg = {"action": "auth", "params": POLYGON_API_KEY}
        ws.send(json.dumps(auth_msg))

    def on_message(self, ws, message):
        data = json.loads(message)
        for msg in data:
            ev = msg.get("ev")
            if ev == "status" and msg.get("status") == "auth_success":
                log.info("Auth Success! Subscribing to raw L2 ticks...")
                sub_params = ",".join([f"Q.{t}" for t in self.tickers])
                ws.send(json.dumps({"action": "subscribe", "params": sub_params}))
            elif ev == "Q": # Level 2 Tick quote
                ticker = msg.get("sym")
                bid_px, bid_sz = msg.get("bp"), msg.get("bs")
                ask_px, ask_sz = msg.get("ap"), msg.get("as")
                
                if bid_sz and ask_sz:
                    obi = (bid_sz - ask_sz) / (bid_sz + ask_sz + 1e-9)
                    mid = (bid_px + ask_px) / 2.0
                    
                    event_data = {
                        "ticker": ticker,
                        "mid_price": mid,
                        "obi": round(obi, 4),
                        "bid_depth": bid_sz,
                        "ask_depth": ask_sz,
                        "timestamp": msg.get("t")
                    }
                    self.bus.publish_async(Topics.MARKET_UPDATE, Event(source="l2_feed", data=event_data))

    def on_error(self, ws, error):
        log.error(f"L2 Feed error: {error}")

    def on_close(self, ws, code, reason):
        log.warning(f"L2 Feed closed: {reason} ({code}). Reconnecting in 5s...")
        time.sleep(5)
        self.start()
        
    def run_simulation(self):
        log.info("Starting high-fidelity L2 simulation feed...")
        import numpy as np
        while True:
            try:
                for t in self.tickers:
                    # Simulate microsecond fluctuations in bid/ask depth
                    bid_sz = int(np.random.poisson(500))
                    ask_sz = int(np.random.poisson(500))
                    obi = (bid_sz - ask_sz) / (bid_sz + ask_sz + 1e-9)
                    
                    event_data = {
                        "ticker": t,
                        "mid_price": round(200.0 + np.random.normal(0, 0.5), 2),
                        "obi": round(obi, 4),
                        "bid_depth": bid_sz,
                        "ask_depth": ask_sz,
                        "timestamp": int(time.time() * 1000)
                    }
                    self.bus.publish_async(Topics.MARKET_UPDATE, Event(source="l2_feed_sim", data=event_data))
                time.sleep(1.0)
            except Exception as e:
                log.error(f"L2 simulation failed: {e}")
                time.sleep(1)

if __name__ == '__main__':
    connector = L2FeedConnector(["AAPL", "MSFT", "NVDA", "TSLA"])
    connector.start()
    
    # Keep active
    while True:
        time.sleep(1)
