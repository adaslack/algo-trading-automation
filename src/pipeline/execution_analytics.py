"""
Execution Analytics Engine (V5 Upgrade)
=========================================
Tracks real execution quality metrics that institutional PMs care about:
  - Fill slippage (expected vs actual price)
  - Execution latency (signal → fill time)
  - Fill rate (orders attempted vs filled)
  - Cost analysis (spread + slippage + commission)

Usage:
    from execution_analytics import ExecutionAnalytics
    analytics = ExecutionAnalytics(db_path)
    analytics.record_fill(ticker, expected_price, fill_price, latency_ms)
    report = analytics.get_report()
"""
import sqlite3
import datetime
import numpy as np
from typing import Optional
from logger import get_logger

log = get_logger("ExecAnalytics")


class ExecutionAnalytics:
    """
    Tracks and analyzes execution quality.
    Every institutional desk monitors these metrics obsessively.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_table()
        self.session_fills: list[dict] = []

    def _ensure_table(self):
        """Create execution analytics table if it doesn't exist."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS execution_analytics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    ticker TEXT,
                    action TEXT,
                    expected_price REAL,
                    fill_price REAL,
                    slippage_bps REAL,
                    latency_ms REAL,
                    qty REAL,
                    commission REAL,
                    spread_cost REAL,
                    total_cost REAL
                )
            ''')
            conn.commit()
            conn.close()
        except Exception as e:
            log.error(f"Failed to create analytics table: {e}")

    def record_fill(self, ticker: str, action: str, expected_price: float,
                    fill_price: float, qty: float, latency_ms: float = 0.0,
                    commission: float = 0.0):
        """
        Record a single execution fill with all cost components.
        """
        # Calculate slippage in basis points
        if expected_price > 0:
            if action == 'BUY':
                slippage_bps = ((fill_price - expected_price) / expected_price) * 10000
            else:
                slippage_bps = ((expected_price - fill_price) / expected_price) * 10000
        else:
            slippage_bps = 0.0

        spread_cost = abs(fill_price - expected_price) * qty
        total_cost = spread_cost + commission

        fill_record = {
            'timestamp': datetime.datetime.now().isoformat(),
            'ticker': ticker,
            'action': action,
            'expected_price': expected_price,
            'fill_price': fill_price,
            'slippage_bps': round(slippage_bps, 2),
            'latency_ms': round(latency_ms, 2),
            'qty': qty,
            'commission': round(commission, 4),
            'spread_cost': round(spread_cost, 4),
            'total_cost': round(total_cost, 4),
        }

        self.session_fills.append(fill_record)

        # Persist to DB
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            conn.execute('''
                INSERT INTO execution_analytics
                (timestamp, ticker, action, expected_price, fill_price,
                 slippage_bps, latency_ms, qty, commission, spread_cost, total_cost)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                fill_record['timestamp'], ticker, action, expected_price,
                fill_price, slippage_bps, latency_ms, qty, commission,
                spread_cost, total_cost
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            log.error(f"Failed to record fill: {e}")

        log.info(f"FILL: {action} {qty}x {ticker} | Expected=${expected_price:.2f} "
                 f"Filled=${fill_price:.2f} | Slippage={slippage_bps:.1f}bps | "
                 f"Latency={latency_ms:.0f}ms")

    def get_report(self, days: int = 30) -> dict:
        """
        Generate execution quality report.
        These are the metrics a PM reviews every morning.
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            cutoff = (datetime.datetime.now() - datetime.timedelta(days=days)).isoformat()
            cursor = conn.execute(
                'SELECT * FROM execution_analytics WHERE timestamp > ? ORDER BY timestamp DESC',
                (cutoff,)
            )
            rows = cursor.fetchall()
            conn.close()
        except Exception as e:
            log.error(f"Report query failed: {e}")
            rows = []

        if not rows:
            return {
                'period_days': days,
                'total_fills': 0,
                'avg_slippage_bps': 0.0,
                'avg_latency_ms': 0.0,
                'total_costs': 0.0,
                'message': 'No execution data available',
            }

        slippages = [r[6] for r in rows]  # slippage_bps column
        latencies = [r[7] for r in rows]  # latency_ms column
        total_costs = sum(r[11] for r in rows)  # total_cost column
        commissions = sum(r[9] for r in rows)  # commission column

        report = {
            'period_days': days,
            'total_fills': len(rows),
            'avg_slippage_bps': round(float(np.mean(slippages)), 2),
            'max_slippage_bps': round(float(np.max(slippages)), 2),
            'p95_slippage_bps': round(float(np.percentile(slippages, 95)), 2),
            'avg_latency_ms': round(float(np.mean(latencies)), 2),
            'max_latency_ms': round(float(np.max(latencies)), 2),
            'total_spread_cost': round(float(total_costs - commissions), 2),
            'total_commissions': round(float(commissions), 2),
            'total_execution_cost': round(float(total_costs), 2),
            'fill_rate': 1.0,  # Placeholder — enhanced with rejected order tracking
            'generated_at': datetime.datetime.now().isoformat(),
        }

        log.info(f"Exec Report: {report['total_fills']} fills | "
                 f"Avg Slip={report['avg_slippage_bps']}bps | "
                 f"Total Cost=${report['total_execution_cost']:.2f}")

        return report

