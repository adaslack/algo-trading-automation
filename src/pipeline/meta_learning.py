"""
Meta-Learning — Dynamic Bayesian Allocator
===========================================
Monitors all strategies and dynamically adjusts their weights
using Bayesian updating instead of simple incremental shifts.
"""
import sqlite3
import time
import os
import numpy as np
from logger    import get_logger
from event_bus import get_bus, Event, Topics

# ── Bayesian Portfolio ────────────────────────────────────────────────────────
from portfolio import get_portfolio as _get_portfolio

log = get_logger("Meta Learning")

DB_PATH     = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data', 'trading_brain.db')
TRADES_FILE = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'pipeline_trades.csv'))
POLL_INTERVAL = 60

ALL_STRATEGIES = [
    'factor_mean_reversion',
    'factor_latent_alpha',
    'factor_microstructure_flow',
    'factor_unified_expected_return'
]

def update_strategy_metrics(cursor):
    for strategy in ALL_STRATEGIES:
        cursor.execute('''
            SELECT pnl_pct FROM trade_history 
            WHERE strategy = ? AND status = 'CLOSED' AND pnl_pct IS NOT NULL
            ORDER BY exit_date DESC LIMIT 50
        ''', (strategy,))
        trades = cursor.fetchall()
        if not trades:
            continue
        returns   = [t[0] for t in trades]
        wins      = [r for r in returns if r > 0]
        losses    = [r for r in returns if r <= 0]
        win_rate  = len(wins) / len(returns) if returns else 0.5
        avg_win   = np.mean(wins)   if wins   else 0.02
        avg_loss  = abs(np.mean(losses)) if losses else 0.01
        sharpe    = (np.mean(returns) / (np.std(returns) + 1e-8)) * np.sqrt(252) if len(returns) > 1 else 1.0
        total_wins   = sum(wins)   if wins   else 0
        total_losses = abs(sum(losses)) if losses else 1
        profit_factor = total_wins / total_losses if total_losses > 0 else 1.0
        cumulative  = np.cumsum(returns)
        running_max = np.maximum.accumulate(cumulative)
        drawdowns   = cumulative - running_max
        max_drawdown = abs(np.min(drawdowns)) if len(drawdowns) > 0 else 0
        cursor.execute('''
            UPDATE strategy_metrics 
            SET win_rate = ?, sharpe_ratio = ?, total_trades = ?, 
                avg_win = ?, avg_loss = ?, max_drawdown = ?, profit_factor = ?
            WHERE strategy = ?
        ''', (win_rate, sharpe, len(returns), avg_win, avg_loss, max_drawdown, profit_factor, strategy))
    return True


def bayesian_weight_update(cursor):
    """Softmax over Sharpe — kept for strategy_weights table; primary sizing is portfolio.py."""
    cursor.execute('SELECT strategy, sharpe_ratio, win_rate, total_trades FROM strategy_metrics')
    metrics = cursor.fetchall()
    if not metrics:
        return
    scores = {}
    for strategy, sharpe, win_rate, total_trades in metrics:
        reliability = min(1.0, np.sqrt(max(total_trades or 0, 1)) / 10.0)
        score = (sharpe or 1.0) * reliability * (win_rate or 0.5)
        scores[strategy] = max(0.1, score)
    temperature = 2.0
    values = np.array(list(scores.values()))
    exp_values = np.exp(values / temperature)
    softmax = exp_values / np.sum(exp_values)
    new_weights = {}
    for i, strategy in enumerate(scores.keys()):
        new_weights[strategy] = max(0.05, min(0.20, softmax[i]))
    total = sum(new_weights.values())
    for s in new_weights:
        new_weights[s] = round(new_weights[s] / total, 4)
    for strategy, weight in new_weights.items():
        cursor.execute('UPDATE strategy_weights SET weight = ? WHERE strategy = ?', (weight, strategy))
    sorted_strats = sorted(new_weights.items(), key=lambda x: x[1], reverse=True)
    log.info(f"Softmax weights (DB only) — Top 3: {[(s, f'{w*100:.1f}%') for s, w in sorted_strats[:3]]}")


def update_closed_trades(cursor):
    cursor.execute('''
        SELECT id, ticker, entry_price, quantity, action FROM trade_history 
        WHERE status = 'CLOSING'
    ''')
    closing_trades = cursor.fetchall()
    closed_records = []
    for trade_id, ticker, entry_price, quantity, action in closing_trades:
        cursor.execute('SELECT close_price FROM market_data WHERE ticker = ?', (ticker,))
        price_row  = cursor.fetchone()
        exit_price = price_row[0] if price_row else entry_price
        if action == 'SELL':
            pnl     = (entry_price - exit_price) * quantity
            pnl_pct = (entry_price - exit_price) / entry_price if entry_price > 0 else 0
        else:
            pnl     = (exit_price - entry_price) * quantity
            pnl_pct = (exit_price - entry_price) / entry_price if entry_price > 0 else 0
        cursor.execute('''
            UPDATE trade_history 
            SET exit_price = ?, exit_date = ?, pnl = ?, pnl_pct = ?, status = 'CLOSED'
            WHERE id = ?
        ''', (exit_price, time.strftime('%Y-%m-%dT%H:%M:%S'), pnl, pnl_pct, trade_id))
        cursor.execute('SELECT strategy FROM trade_history WHERE id = ?', (trade_id,))
        strat_row = cursor.fetchone()
        strategy  = strat_row[0] if strat_row else 'Unknown'
        outcome   = 'WIN' if pnl_pct > 0 else 'LOSS'
        cursor.execute('''
            INSERT INTO strategy_performance (ticker, strategy, signal_date, outcome, pnl, pnl_pct, holding_days)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (ticker, strategy, time.strftime('%Y-%m-%d'), outcome, pnl, pnl_pct, 0))
        log.info(f"Trade closed: {ticker} {outcome} P&L={pnl_pct*100:+.1f}% (Strategy: {strategy})")
        closed_records.append((strategy, pnl_pct))
    return closed_records


def on_order_filled(event: Event):
    log.info("⚡ Fill captured — running Bayesian posterior update + softmax reallocation...")
    try:
        conn   = sqlite3.connect(DB_PATH, timeout=15.0)
        cursor = conn.cursor()

        # 1. Mark closing trades as CLOSED, compute P&L
        closed_records = update_closed_trades(cursor)

        # 2. ── BAYESIAN POSTERIOR UPDATE ───────────────────────────────────
        # Feed each closed trade return into portfolio.BayesianPortfolio
        portfolio = _get_portfolio()
        for strategy, pnl_pct in closed_records:
            portfolio.update(strategy=strategy, realised_return=float(pnl_pct))

        # Persist updated posterior summaries to strategy_metrics
        portfolio.persist_to_db(db_path=DB_PATH)

        # 3. Update DB strategy_metrics & softmax weights (legacy DB table)
        update_strategy_metrics(cursor)
        bayesian_weight_update(cursor)

        conn.commit()
        conn.close()

        # Log posterior report
        report = portfolio.report()
        log.info("Posterior beliefs after fill:")
        for r in report:
            log.info(
                f"  {r['strategy']}: μ={r['posterior_mu']*100:+.2f}% "
                f"σ={r['posterior_sigma']*100:.2f}% n={r['n_trades']} "
                f"Sharpe≈{r['sharpe_proxy']:.2f}"
            )
        log.info("⚡ Bayesian update complete.")
    except Exception as e:
        log.error(f"Meta-learning real-time update failed: {e}")


def run_meta_learning():
    log.info("Starting Dynamic Bayesian Allocator...")
    
    # Subscribe to ORDER_FILLED for real-time dynamic reallocations
    bus = get_bus()
    bus.subscribe(Topics.ORDER_FILLED, on_order_filled)
    
    while True:
        try:
            conn = sqlite3.connect(DB_PATH, timeout=15.0)
            cursor = conn.cursor()
            
            # Periodic audits
            update_closed_trades(cursor)
            update_strategy_metrics(cursor)
            bayesian_weight_update(cursor)
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            log.error(f"Meta learning loop failed: {e}")
        
        time.sleep(POLL_INTERVAL)

if __name__ == '__main__':
    run_meta_learning()

