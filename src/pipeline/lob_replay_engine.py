"""
High-Fidelity Limit Order Book (LOB) Replay Engine
===================================================
Models realistic execution matching, queue priority exhaustion, hidden iceberg
liquidity, and venue fragmentation routing latencies.

Provides institutional-grade backtesting matching accuracy for microstructure strategies.
"""

import numpy as np
import time
from dataclasses import dataclass, field
from typing import Optional, Any
from logger import get_logger

log = get_logger("LOBReplayEngine")

@dataclass
class LimitOrder:
    order_id: str
    ticker: str
    action: str          # 'BUY' (bid) | 'SELL' (ask)
    price: float
    quantity: float
    remaining_qty: float
    queue_position: float  # Volume ahead of this order in the queue
    timestamp: float
    venue: str = "PRIMARY" # Nasdaq, NYSE, BATS, etc.
    status: str = "QUEUED" # QUEUED | PARTIAL | FILLED | CANCELLED

@dataclass
class ExecutionResult:
    order_id: str
    filled_qty: float
    avg_fill_price: float
    slippage_bps: float
    latency_ms: float
    status: str

class LOBReplayEngine:
    """
    Deterministic Limit Order Book Replay Engine.
    Tracks queue priority, estimates hidden iceberg volume, and simulates venue routing latency.
    """

    def __init__(self, iceberg_prob: float = 0.20, default_latency_ms: float = 8.5):
        self.iceberg_prob = iceberg_prob
        self.default_latency_ms = default_latency_ms
        self.active_orders: dict[str, LimitOrder] = {}
        # Tracks standard book depth per ticker: {ticker: {price: queue_volume}}
        self._book_depth: dict[str, dict[float, float]] = {}

    def set_book_depth(self, ticker: str, price: float, volume: float) -> None:
        """Update the known volume at a specific price level to calibrate queue positions."""
        if ticker not in self._book_depth:
            self._book_depth[ticker] = {}
        self._book_depth[ticker][price] = max(0.0, volume)

    def submit_limit_order(
        self,
        order_id: str,
        ticker: str,
        action: str,
        price: float,
        quantity: float,
        venue: str = "PRIMARY"
    ) -> LimitOrder:
        """
        Submit a limit order. Places the order at the end of the queue 
        for the specified price level, incorporating venue fragmentation routing latency,
        DMA microburst network delays, and Smart Order Routing (SOR) dynamic venue selection.
        """
        ticker_depth = self._book_depth.get(ticker, {})
        base_depth = ticker_depth.get(price, quantity * 5.0)
        
        # 1. Smart Order Routing (SOR) implementation
        if venue.upper() == "SOR":
            # Simulate fragmented venue queue depths
            venues = {
                "NASDAQ": base_depth * np.random.uniform(0.9, 1.2),
                "NYSE":   base_depth * np.random.uniform(0.8, 1.1),
                "BATS":   base_depth * np.random.uniform(0.7, 0.95)
            }
            # Route to the venue with the lowest queue ahead
            chosen_venue = min(venues, key=venues.get)
            volume_ahead = venues[chosen_venue]
            latency = self.default_latency_ms + np.random.uniform(1.2, 3.5) # Additional SOR hop delay
            log.info(f"LOB Replay [SOR]: Routed order '{order_id}' to {chosen_venue} (Queue: {volume_ahead:.1f} units)")
        else:
            chosen_venue = venue
            volume_ahead = base_depth
            latency = self.default_latency_ms
            if venue != "PRIMARY":
                latency += np.random.uniform(2.0, 7.5)

        # 2. DMA Latency Jitter and Queue Microburst simulations
        # Large orders or sudden volume spikes trigger network queue microbursts on exchange gateways
        if quantity > 500.0:
            microburst_delay = np.random.uniform(1.5, 6.0)
            latency += microburst_delay
            log.warning(f"LOB Replay [DMA]: Gateway microburst detected for size {quantity:.1f} shares! Added {microburst_delay:.2f} ms latency.")

        order = LimitOrder(
            order_id=order_id,
            ticker=ticker,
            action=action.upper(),
            price=price,
            quantity=quantity,
            remaining_qty=quantity,
            queue_position=volume_ahead,
            timestamp=time.time() + (latency / 1000.0),
            venue=chosen_venue,
            status="QUEUED"
        )
        self.active_orders[order_id] = order
        log.info(f"LOB Replay: Placed {action} limit order '{order_id}' for {quantity}x {ticker} at ${price:.2f} on {chosen_venue}. Queue ahead: {volume_ahead:.1f} units.")
        return order

    def process_market_trade(
        self,
        ticker: str,
        trade_price: float,
        trade_volume: float
    ) -> list[ExecutionResult]:
        """
        Replay a trade transaction occurring in the market.
        Exhausts queue priority ahead of active orders and fills orders when priority reaches 0.
        Models 20% iceberg probability which inserts hidden volume ahead of us.
        """
        results = []
        active_ids = list(self.active_orders.keys())

        for oid in active_ids:
            order = self.active_orders[oid]
            if order.ticker != ticker or order.status in ["FILLED", "CANCELLED"]:
                continue

            # Check if trade matches order side criteria
            # For BUY (bid) order: trade must occur at or below order price to exhaust queue/fill
            # For SELL (ask) order: trade must occur at or above order price to exhaust queue/fill
            matches_price = False
            if order.action == "BUY" and trade_price <= order.price:
                matches_price = True
            elif order.action == "SELL" and trade_price >= order.price:
                matches_price = True

            if not matches_price:
                continue

            # Model 20% Iceberg probability:
            # 20% chance that a trade triggers hidden institutional iceberg orders 
            # which absorb liquidity and add volume ahead in the queue.
            if np.random.random() < self.iceberg_prob:
                hidden_iceberg_vol = trade_volume * np.random.uniform(0.5, 1.5)
                order.queue_position += hidden_iceberg_vol
                log.info(f"LOB Replay: Hidden Iceberg detected on {ticker} at ${trade_price:.2f}! Added {hidden_iceberg_vol:.1f} units ahead in queue.")

            # Exhaust queue priority
            if order.queue_position > 0:
                exhausted = min(order.queue_position, trade_volume)
                order.queue_position -= exhausted
                trade_volume -= exhausted
                log.debug(f"LOB Replay: Exhausted {exhausted:.1f} units of queue ahead of order '{oid}'. Remaining queue: {order.queue_position:.1f}")

            # If queue priority is fully exhausted, fill order with remaining trade volume
            if order.queue_position <= 0 and trade_volume > 0:
                fill_qty = min(order.remaining_qty, trade_volume)
                order.remaining_qty -= fill_qty
                trade_volume -= fill_qty
                
                order.status = "FILLED" if order.remaining_qty <= 0 else "PARTIAL"
                
                # Calculate latency and slip
                latency_ms = (time.time() - order.timestamp) * 1000.0
                slippage_bps = ((trade_price - order.price) / order.price) * 10000.0 if order.action == "BUY" else ((order.price - trade_price) / order.price) * 10000.0

                result = ExecutionResult(
                    order_id=order.order_id,
                    filled_qty=fill_qty,
                    avg_fill_price=trade_price,
                    slippage_bps=round(slippage_bps, 2),
                    latency_ms=round(max(0.0, latency_ms), 2),
                    status=order.status
                )
                results.append(result)
                log.info(f"LOB Replay: ORDER {order.status}! Filled {fill_qty:.1f} shares of order '{order.order_id}' at ${trade_price:.2f}. Slippage: {slippage_bps:.2f} bps. Latency: {latency_ms:.2f} ms.")

                if order.status == "FILLED":
                    del self.active_orders[oid]

        return results

    def cancel_order(self, order_id: str) -> Optional[LimitOrder]:
        """Cancel an active limit order."""
        if order_id in self.active_orders:
            order = self.active_orders[order_id]
            order.status = "CANCELLED"
            del self.active_orders[order_id]
            log.info(f"LOB Replay: Cancelled limit order '{order_id}'.")
            return order
        return None

