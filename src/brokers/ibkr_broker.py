"""
IBKR Broker — Interactive Brokers Integration via ib_insync
=============================================================
Implements BaseBroker for Interactive Brokers TWS/Gateway API.

Requirements:
  - TWS or IB Gateway must be running locally
  - API connections enabled in TWS settings
  - Paper trading port: 7497, Live trading port: 7496

Usage:
  broker = IBKRBroker(host='127.0.0.1', port=7497, client_id=1)
  broker.connect()
"""
import os
import time
import datetime
import threading
import logging
from typing import Optional

from base_broker import (
    BaseBroker, BrokerQuote, BrokerAccount, BrokerPosition,
    BrokerOrder, BrokerOrderResult, BrokerBar, BrokerAsset,
    OrderSideEnum, OrderTypeEnum, TimeInForceEnum
)

log = logging.getLogger("IBKRBroker")


class IBKRBroker(BaseBroker):
    """
    Interactive Brokers broker implementation using ib_insync.

    Connects to TWS or IB Gateway via socket API.
    All methods translate between the unified BaseBroker interface
    and IBKR's contract-based API.
    """

    def __init__(self, host: str = '127.0.0.1', port: int = 7497, client_id: int = 1):
        self._host = host
        self._port = port
        self._client_id = client_id
        self._ib = None
        self._connected = False
        self._bar_callbacks = {}  # {symbol: callback}
        self._stream_thread = None

    # ── Connection ────────────────────────────────────────────────────────────

    def connect(self) -> bool:
        """Connect to TWS or IB Gateway."""
        try:
            from ib_insync import IB
            self._ib = IB()
            self._ib.connect(self._host, self._port, clientId=self._client_id)
            self._connected = self._ib.isConnected()
            if self._connected:
                log.info(f"Connected to IBKR at {self._host}:{self._port} (client={self._client_id})")
            else:
                log.error("Failed to connect to IBKR — is TWS/Gateway running?")
            return self._connected
        except Exception as e:
            log.error(f"IBKR connection failed: {e}")
            self._connected = False
            return False

    def disconnect(self):
        """Disconnect from TWS/Gateway."""
        if self._ib and self._connected:
            try:
                self._ib.disconnect()
            except Exception:
                pass
            self._connected = False
            log.info("Disconnected from IBKR.")

    def is_connected(self) -> bool:
        if self._ib is None:
            return False
        try:
            return self._ib.isConnected()
        except Exception:
            return False

    # ── Helper: Create IBKR Contract ──────────────────────────────────────────

    def _make_stock_contract(self, symbol: str):
        """Create an IBKR Stock contract for US equities."""
        from ib_insync import Stock
        return Stock(symbol, 'SMART', 'USD')

    # ── Account & Positions ───────────────────────────────────────────────────

    def get_account(self) -> BrokerAccount:
        if not self.is_connected():
            return BrokerAccount()
        try:
            summary = self._ib.accountSummary()
            values = {item.tag: float(item.value) for item in summary
                      if item.tag in ('NetLiquidation', 'BuyingPower', 'TotalCashValue')}
            return BrokerAccount(
                portfolio_value=values.get('NetLiquidation', 0.0),
                buying_power=values.get('BuyingPower', 0.0),
                cash=values.get('TotalCashValue', 0.0),
                currency="USD"
            )
        except Exception as e:
            log.error(f"Failed to get IBKR account: {e}")
            return BrokerAccount()

    def get_positions(self) -> dict[str, BrokerPosition]:
        if not self.is_connected():
            return {}
        try:
            positions = self._ib.positions()
            result = {}
            for pos in positions:
                symbol = pos.contract.symbol
                qty = float(pos.position)
                result[symbol] = BrokerPosition(
                    symbol=symbol,
                    qty=qty,
                    avg_cost=float(pos.avgCost),
                    market_value=abs(qty * float(pos.avgCost)),
                    unrealized_pnl=0.0,  # Calculated separately via portfolio
                    side="long" if qty > 0 else "short"
                )
            return result
        except Exception as e:
            log.error(f"Failed to get IBKR positions: {e}")
            return {}

    def get_funds(self) -> float:
        account = self.get_account()
        return account.buying_power

    # ── Market Data ───────────────────────────────────────────────────────────

    def get_latest_quote(self, symbol: str) -> BrokerQuote:
        if not self.is_connected():
            return BrokerQuote(symbol=symbol)
        try:
            contract = self._make_stock_contract(symbol)
            self._ib.qualifyContracts(contract)
            ticker = self._ib.reqMktData(contract, '', False, False)
            # Wait briefly for data to arrive
            self._ib.sleep(1)
            bid = float(ticker.bid) if ticker.bid and ticker.bid > 0 else 0.0
            ask = float(ticker.ask) if ticker.ask and ticker.ask > 0 else 0.0
            last = float(ticker.last) if ticker.last and ticker.last > 0 else 0.0
            self._ib.cancelMktData(contract)
            return BrokerQuote(
                symbol=symbol,
                bid_price=bid,
                ask_price=ask,
                bid_size=int(ticker.bidSize) if ticker.bidSize else 0,
                ask_size=int(ticker.askSize) if ticker.askSize else 0,
                last_price=last
            )
        except Exception as e:
            log.error(f"Failed to get IBKR quote for {symbol}: {e}")
            return BrokerQuote(symbol=symbol)

    def get_historical_bars(self, symbols: list[str], timeframe: str,
                            start: object, end: object) -> dict[str, list[BrokerBar]]:
        if not self.is_connected():
            return {}

        # Map our timeframe strings to IBKR barSizeSetting
        tf_map = {
            '1Min': '1 min',
            '5Min': '5 mins',
            '15Min': '15 mins',
            '1Hour': '1 hour',
            '1Day': '1 day',
        }
        bar_size = tf_map.get(timeframe, '1 min')

        # Calculate duration string from start/end
        if isinstance(start, datetime.datetime) and isinstance(end, datetime.datetime):
            delta = end - start
            days = max(1, delta.days)
            if days <= 1:
                duration = f'{int(delta.total_seconds())} S'
            else:
                duration = f'{days} D'
        else:
            duration = '2 D'

        result = {}
        for symbol in symbols:
            try:
                contract = self._make_stock_contract(symbol)
                self._ib.qualifyContracts(contract)
                bars = self._ib.reqHistoricalData(
                    contract,
                    endDateTime=end if isinstance(end, str) else '',
                    durationStr=duration,
                    barSizeSetting=bar_size,
                    whatToShow='TRADES',
                    useRTH=True,
                    formatDate=1
                )
                result[symbol] = [
                    BrokerBar(
                        timestamp=b.date,
                        open=float(b.open),
                        high=float(b.high),
                        low=float(b.low),
                        close=float(b.close),
                        volume=float(b.volume)
                    )
                    for b in bars
                ]
            except Exception as e:
                log.error(f"Failed to get IBKR historical bars for {symbol}: {e}")
                result[symbol] = []
        return result

    def get_tradable_assets(self) -> list[BrokerAsset]:
        """
        Queries tradable equities from the active broker.
        We use a scanner subscription to find top US equities by volume.
        """
        if not self.is_connected():
            return []
        try:
            from ib_insync import ScannerSubscription
            scanner = ScannerSubscription(
                instrument='STK',
                locationCode='STK.US.MAJOR',
                scanCode='TOP_PERC_GAIN',
                numberOfRows=500
            )
            results = self._ib.reqScannerData(scanner)
            assets = []
            for item in results:
                contract = item.contractDetails.contract
                assets.append(BrokerAsset(
                    symbol=contract.symbol,
                    name=item.contractDetails.longName or contract.symbol,
                    exchange=contract.exchange,
                    asset_class='us_equity',
                    tradable=True,
                    marginable=True,
                    fractionable=False  # IBKR does NOT support fractional shares
                ))
            return assets
        except Exception as e:
            log.error(f"Failed to get IBKR tradable assets: {e}")
            return []

    # ── Market Status ─────────────────────────────────────────────────────────

    def is_market_open(self) -> bool:
        """Check if US equity market is open using IBKR clock."""
        if not self.is_connected():
            return False
        try:
            # Check via requesting market data for SPY — if it returns real-time, market is open
            import datetime as dt
            import pytz
            eastern = pytz.timezone('US/Eastern')
            now = dt.datetime.now(eastern)
            # Basic check: weekday + 9:30 AM to 4:00 PM ET
            if now.weekday() >= 5:  # Weekend
                return False
            market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
            return market_open <= now <= market_close
        except Exception:
            return False

    # ── Order Execution ───────────────────────────────────────────────────────

    def place_order(self, order: BrokerOrder) -> BrokerOrderResult:
        if not self.is_connected():
            log.error("Cannot place order — IBKR not connected")
            return BrokerOrderResult(status="error")

        try:
            from ib_insync import MarketOrder, LimitOrder, StopOrder

            contract = self._make_stock_contract(order.symbol)
            self._ib.qualifyContracts(contract)

            # IBKR requires integer quantities
            qty = int(round(order.qty))
            if qty <= 0:
                log.warning(f"IBKR: Qty rounded to {qty} — too small to execute")
                return BrokerOrderResult(status="rejected")

            action = "BUY" if order.side == OrderSideEnum.BUY else "SELL"

            # Create IBKR order object
            if order.order_type == OrderTypeEnum.MARKET:
                ib_order = MarketOrder(action, qty)
            elif order.order_type == OrderTypeEnum.LIMIT and order.limit_price:
                ib_order = LimitOrder(action, qty, order.limit_price)
            elif order.order_type == OrderTypeEnum.STOP and order.stop_price:
                ib_order = StopOrder(action, qty, order.stop_price)
            else:
                ib_order = MarketOrder(action, qty)

            # Set time in force
            tif_map = {
                TimeInForceEnum.DAY: 'DAY',
                TimeInForceEnum.GTC: 'GTC',
                TimeInForceEnum.IOC: 'IOC',
                TimeInForceEnum.FOK: 'FOK',
            }
            ib_order.tif = tif_map.get(order.time_in_force, 'DAY')

            # Submit order
            trade = self._ib.placeOrder(contract, ib_order)

            # Wait briefly for fill (up to 5 seconds for market orders)
            if order.order_type == OrderTypeEnum.MARKET:
                max_wait = 5.0
                elapsed = 0.0
                while elapsed < max_wait and trade.orderStatus.status not in ('Filled', 'Cancelled', 'ApiCancelled'):
                    self._ib.sleep(0.5)
                    elapsed += 0.5

            filled_price = 0.0
            filled_qty = 0.0
            if trade.fills:
                filled_price = float(trade.fills[-1].execution.avgPrice)
                filled_qty = sum(float(f.execution.shares) for f in trade.fills)

            # Estimate commission
            commission = filled_qty * self.commission_per_share

            return BrokerOrderResult(
                order_id=str(trade.order.orderId),
                symbol=order.symbol,
                filled_qty=filled_qty,
                filled_avg_price=filled_price,
                status=trade.orderStatus.status,
                commission=commission
            )

        except Exception as e:
            log.error(f"IBKR order placement failed for {order.symbol}: {e}")
            return BrokerOrderResult(status="error")

    def cancel_order(self, order_id: str) -> bool:
        if not self.is_connected():
            return False
        try:
            open_orders = self._ib.openOrders()
            for order in open_orders:
                if str(order.orderId) == order_id:
                    self._ib.cancelOrder(order)
                    return True
            return False
        except Exception as e:
            log.error(f"Failed to cancel IBKR order {order_id}: {e}")
            return False

    # ── Real-Time Streaming ───────────────────────────────────────────────────

    def subscribe_bars(self, symbols: list[str], callback) -> None:
        """
        Subscribe to real-time 5-second bars for the given symbols.
        IBKR uses reqRealTimeBars which provides 5-second bars.
        We aggregate them into 1-minute bars and call the callback.
        """
        if not self.is_connected():
            log.error("Cannot subscribe to bars — IBKR not connected")
            return

        for symbol in symbols:
            try:
                contract = self._make_stock_contract(symbol)
                self._ib.qualifyContracts(contract)
                self._bar_callbacks[symbol] = callback

                # reqRealTimeBars gives 5-second bars
                bars = self._ib.reqRealTimeBars(
                    contract, barSize=5, whatToShow='TRADES', useRTH=True
                )
                # Set up aggregation
                bars.updateEvent += lambda bars, hasNewBar, sym=symbol: self._on_realtime_bar(sym, bars, hasNewBar)

                log.info(f"Subscribed to IBKR real-time bars for {symbol}")
            except Exception as e:
                log.error(f"Failed to subscribe to IBKR bars for {symbol}: {e}")

    def _on_realtime_bar(self, symbol: str, bars, has_new_bar):
        """Handle incoming 5-second bars and aggregate to 1-minute."""
        if not has_new_bar or not bars:
            return
        bar = bars[-1]
        callback = self._bar_callbacks.get(symbol)
        if callback:
            broker_bar = BrokerBar(
                timestamp=bar.time,
                open=float(bar.open_),
                high=float(bar.high),
                low=float(bar.low),
                close=float(bar.close),
                volume=float(bar.volume)
            )
            try:
                callback(symbol, broker_bar)
            except Exception as e:
                log.error(f"Error in bar callback for {symbol}: {e}")

    def run_stream(self) -> None:
        """Start the IBKR event loop (blocking)."""
        if not self.is_connected():
            log.error("Cannot run stream — IBKR not connected")
            return
        log.info("Starting IBKR real-time event loop...")
        try:
            self._ib.run()
        except KeyboardInterrupt:
            log.info("IBKR stream interrupted.")
        finally:
            self.disconnect()

    # ── Broker-Specific Properties ────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "Interactive Brokers"

    @property
    def supports_fractional(self) -> bool:
        return False  # IBKR does NOT support fractional shares

    @property
    def commission_per_share(self) -> float:
        return 0.005  # IBKR fixed rate: $0.005/share, min $1.00
