"""
Risk Gate — Centralized Risk Authority (V4 Upgrade)
=====================================================
This module provides a single, centralized risk approval gate.
NOTHING executes without passing through RiskGate.approve().

This replaces the distributed risk checks that were previously
scattered across Wheel 3, Wheel 4, and Wheel 6.

Usage:
    from risk_gate import RiskGate
    gate = RiskGate(db_path, trading_client)
    decision = gate.approve(order)
    if decision.approved:
        execute(order)
"""
import sqlite3
import datetime
import numpy as np
from typing import Optional
from logger import get_logger
from models import (
    Order, RiskDecision, CircuitBreakerState,
    SignalType, OrderStatus
)

log = get_logger("RiskGate")


class RiskGate:
    """
    Single centralized risk authority.
    All orders MUST pass through approve() before execution.
    No wheel is allowed to bypass this gate.
    """

    # --- Risk Limits ---
    MAX_POSITIONS = 15
    MAX_DAILY_TRADES = 20
    MAX_SECTOR_EXPOSURE = 0.25       # 25% per sector
    MAX_SINGLE_POSITION = 0.075      # 7.5% per position
    CIRCUIT_BREAKER_PCT = 0.03       # 3% daily loss halts buying
    MAX_CORRELATION = 0.70           # Reject if avg corr > 0.7
    MIN_ALLOCATION_PCT = 0.01        # Floor: 1%
    MAX_ALLOCATION_PCT = 0.05        # Cap: 5%

    def __init__(self, db_path: str, trading_client=None):
        self.db_path = db_path
        self.trading_client = trading_client

    def approve(self, order: Order) -> RiskDecision:
        """
        Central risk approval gate.
        Returns a RiskDecision with approved=True/False and reasons.
        """
        checks = []

        # 1. Circuit Breaker
        cb_clear, cb_reason = self._check_circuit_breaker()
        checks.append(("circuit_breaker", cb_clear, cb_reason))

        # 2. Position Limit
        pos_clear, pos_reason = self._check_position_limit(order)
        checks.append(("position_limit", pos_clear, pos_reason))

        # 3. Sector Concentration
        sec_clear, sec_reason = self._check_sector_limit(order)
        checks.append(("sector_limit", sec_clear, sec_reason))

        # 4. Single Position Size
        size_clear, size_reason = self._check_position_size(order)
        checks.append(("position_size", size_clear, size_reason))

        # 5. Daily Trade Count
        trade_clear, trade_reason = self._check_daily_trade_limit()
        checks.append(("daily_trades", trade_clear, trade_reason))

        # Aggregate decision
        all_passed = all(passed for _, passed, _ in checks)
        failed_checks = [(name, reason) for name, passed, reason in checks if not passed]

        if not all_passed:
            rejection_reason = " | ".join([f"{n}: {r}" for n, r in failed_checks])
            log.warning(f"REJECTED {order.action.value} {order.ticker}: {rejection_reason}")
            return RiskDecision(
                approved=False,
                order=order,
                reason=rejection_reason,
                circuit_breaker_clear=cb_clear,
                sector_limit_clear=sec_clear,
                position_limit_clear=pos_clear,
            )

        log.info(f"APPROVED {order.action.value} {order.ticker} | Qty: {order.qty:.2f} | Kelly: {order.kelly_pct*100:.1f}%")
        return RiskDecision(
            approved=True,
            order=order,
            reason="All risk checks passed",
            adjusted_qty=order.qty,
            adjusted_kelly=order.kelly_pct,
            circuit_breaker_clear=True,
            sector_limit_clear=True,
            position_limit_clear=True,
        )

    def _check_circuit_breaker(self) -> tuple[bool, str]:
        """Check if circuit breaker is tripped."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            c = conn.cursor()
            c.execute("SELECT is_halted, halt_reason FROM circuit_breaker WHERE id=1")
            row = c.fetchone()
            conn.close()
            if row and row[0] == 1:
                return False, f"Circuit breaker active: {row[1]}"
        except Exception as e:
            log.error(f"Circuit breaker check failed: {e}")
        return True, "Clear"

    def _check_position_limit(self, order: Order) -> tuple[bool, str]:
        """Check if we're at max positions."""
        if order.action == SignalType.SELL:
            return True, "Sells always allowed"
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM trade_history WHERE exit_date IS NULL")
            count = c.fetchone()[0]
            conn.close()
            if count >= self.MAX_POSITIONS:
                return False, f"At max positions ({count}/{self.MAX_POSITIONS})"
        except Exception as e:
            log.error(f"Position limit check failed: {e}")
        return True, "Clear"

    def _check_sector_limit(self, order: Order) -> tuple[bool, str]:
        """Check sector concentration limit."""
        if order.action == SignalType.SELL:
            return True, "Sells always allowed"
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            c = conn.cursor()
            # Get the sector for this ticker
            c.execute("SELECT sector FROM market_data WHERE ticker=?", (order.ticker,))
            row = c.fetchone()
            if not row:
                conn.close()
                return True, "No sector data"
            sector = row[0]

            # Count positions in this sector
            c.execute("""
                SELECT COUNT(*) FROM trade_history th
                JOIN market_data md ON th.ticker = md.ticker
                WHERE th.exit_date IS NULL AND md.sector = ?
            """, (sector,))
            sector_count = c.fetchone()[0]

            c.execute("SELECT COUNT(*) FROM trade_history WHERE exit_date IS NULL")
            total = c.fetchone()[0]
            conn.close()

            if total > 0 and (sector_count / max(total, 1)) >= self.MAX_SECTOR_EXPOSURE:
                return False, f"Sector {sector} at {sector_count}/{total} ({self.MAX_SECTOR_EXPOSURE*100:.0f}% limit)"
        except Exception as e:
            log.error(f"Sector limit check failed: {e}")
        return True, "Clear"

    def _check_position_size(self, order: Order) -> tuple[bool, str]:
        """Check single position size limit."""
        if order.kelly_pct > self.MAX_ALLOCATION_PCT:
            return False, f"Kelly {order.kelly_pct*100:.1f}% exceeds max {self.MAX_ALLOCATION_PCT*100:.0f}%"
        return True, "Clear"

    def _check_daily_trade_limit(self) -> tuple[bool, str]:
        """Check daily trade count limit."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            c = conn.cursor()
            c.execute("SELECT trades_today FROM circuit_breaker WHERE id=1")
            row = c.fetchone()
            conn.close()
            if row and row[0] >= self.MAX_DAILY_TRADES:
                return False, f"Daily trade limit reached ({row[0]}/{self.MAX_DAILY_TRADES})"
        except Exception as e:
            log.error(f"Trade limit check failed: {e}")
        return True, "Clear"

    def get_circuit_breaker_state(self) -> CircuitBreakerState:
        """Get current circuit breaker state."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            c = conn.cursor()
            c.execute("SELECT is_halted, halt_reason, daily_pnl, daily_pnl_pct, trades_today, day_open_value FROM circuit_breaker WHERE id=1")
            row = c.fetchone()
            conn.close()
            if row:
                return CircuitBreakerState(
                    is_halted=bool(row[0]),
                    halt_reason=row[1] or "",
                    daily_pnl=row[2] or 0.0,
                    daily_pnl_pct=row[3] or 0.0,
                    trades_today=row[4] or 0,
                    day_open_value=row[5] or 0.0,
                )
        except Exception as e:
            log.error(f"Circuit breaker state fetch failed: {e}")
        return CircuitBreakerState()

