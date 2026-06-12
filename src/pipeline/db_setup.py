import os
import duckdb
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DUCKDB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data', 'trading_brain.duckdb')

def setup_database_postgres():
    """Sets up the V3 Institutional schema inside the PostgreSQL database container."""
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME", "trading_brain")
    user = os.getenv("DB_USER", "quant_operator")
    password = os.getenv("DB_PASSWORD", "")
    
    print(f"Connecting to PostgreSQL Database '{dbname}' on {host}:{port}...")
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=dbname,
        user=user,
        password=password
    )
    cursor = conn.cursor()
    
    try:
        # 1. MARKET DATA (Wheel 1)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS market_data (
                ticker VARCHAR(20) PRIMARY KEY,
                sector VARCHAR(100),
                timestamp TIMESTAMP WITH TIME ZONE,
                close_price DOUBLE PRECISION,
                open_price DOUBLE PRECISION,
                high_price DOUBLE PRECISION,
                low_price DOUBLE PRECISION,
                volume DOUBLE PRECISION,
                garch_volatility DOUBLE PRECISION,
                volume_ratio DOUBLE PRECISION,
                vwap DOUBLE PRECISION,
                hurst_exponent DOUBLE PRECISION,
                vpin DOUBLE PRECISION,
                obi DOUBLE PRECISION,
                micro_price DOUBLE PRECISION,
                dealer_gex DOUBLE PRECISION,
                insider_score DOUBLE PRECISION,
                momentum_12_1 DOUBLE PRECISION,
                reversal_5d DOUBLE PRECISION,
                volume_breakout DOUBLE PRECISION,
                vol_regime DOUBLE PRECISION,
                trend_strength DOUBLE PRECISION,
                atr14 DOUBLE PRECISION,
                volatility_20d DOUBLE PRECISION
            )
        ''')

        # V13 dynamic columns migration
        new_cols_pg = [
            ('momentum_12_1', 'DOUBLE PRECISION'),
            ('reversal_5d', 'DOUBLE PRECISION'),
            ('volume_breakout', 'DOUBLE PRECISION'),
            ('vol_regime', 'DOUBLE PRECISION'),
            ('trend_strength', 'DOUBLE PRECISION'),
            ('atr14', 'DOUBLE PRECISION'),
            ('volatility_20d', 'DOUBLE PRECISION')
        ]
        for col_name, col_type in new_cols_pg:
            try:
                cursor.execute(f"ALTER TABLE market_data ADD COLUMN {col_name} {col_type}")
                conn.commit()
            except Exception as e:
                conn.rollback()
                if "already exists" not in str(e).lower():
                    print(f"PostgreSQL migration warning on {col_name}: {e}")

        # 2. ALPHA SIGNALS (Wheel 2)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alpha_signals (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE,
                ticker VARCHAR(20),
                signal_type VARCHAR(20),
                target_strategy VARCHAR(100),
                confidence DOUBLE PRECISION,
                exit_rule VARCHAR(100),
                status VARCHAR(20) DEFAULT 'PENDING'
            )
        ''')

        # 3. EXECUTION QUEUE (Wheel 3)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS execution_queue (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE,
                ticker VARCHAR(20),
                action VARCHAR(20),
                quantity DOUBLE PRECISION,
                price_estimate DOUBLE PRECISION,
                order_type VARCHAR(20) DEFAULT 'MARKET',
                status VARCHAR(20) DEFAULT 'QUEUED'
            )
        ''')

        # 4. STRATEGY WEIGHTS (Wheel 8)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS strategy_weights (
                strategy VARCHAR(100) PRIMARY KEY,
                weight DOUBLE PRECISION
            )
        ''')

        strategies_defaults = [
            ('factor_mean_reversion', 0.25),
            ('factor_latent_alpha', 0.25),
            ('factor_microstructure_flow', 0.25),
            ('factor_unified_expected_return', 0.25),
        ]
        
        for strat, w in strategies_defaults:
            cursor.execute("""
                INSERT INTO strategy_weights (strategy, weight) 
                VALUES (%s, %s) 
                ON CONFLICT (strategy) DO NOTHING
            """, (strat, w))

        # 5. STRATEGY METRICS (Wheel 8)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS strategy_metrics (
                strategy VARCHAR(100) PRIMARY KEY,
                win_rate DOUBLE PRECISION DEFAULT 0.5,
                sharpe_ratio DOUBLE PRECISION DEFAULT 1.0,
                total_trades INTEGER DEFAULT 0,
                avg_win DOUBLE PRECISION DEFAULT 0.0,
                avg_loss DOUBLE PRECISION DEFAULT 0.0,
                max_drawdown DOUBLE PRECISION DEFAULT 0.0,
                profit_factor DOUBLE PRECISION DEFAULT 1.0
            )
        ''')

        for strat, _ in strategies_defaults:
            cursor.execute("""
                INSERT INTO strategy_metrics (strategy) 
                VALUES (%s) 
                ON CONFLICT (strategy) DO NOTHING
            """, (strat,))

        # 6. DAILY WATCHLIST
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_watchlist (
                ticker VARCHAR(20) PRIMARY KEY,
                sector VARCHAR(100)
            )
        ''')

        # 7. MACRO SENTIMENT
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS macro_sentiment (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE,
                vix_level DOUBLE PRECISION,
                macro_score DOUBLE PRECISION,
                top_headline TEXT,
                source VARCHAR(50) DEFAULT 'alpha_vantage'
            )
        ''')

        # 8. OPTIONS QUEUE (Wheel 5)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS options_queue (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE,
                underlying VARCHAR(20),
                contract_symbol VARCHAR(50),
                option_type VARCHAR(20),
                action VARCHAR(20),
                strike DOUBLE PRECISION,
                expiry VARCHAR(20),
                quantity INTEGER,
                premium_estimate DOUBLE PRECISION,
                status VARCHAR(20) DEFAULT 'QUEUED'
            )
        ''')

        # 9. CIRCUIT BREAKER
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS circuit_breaker (
                id INTEGER PRIMARY KEY DEFAULT 1,
                date VARCHAR(20),
                day_open_value DOUBLE PRECISION,
                current_value DOUBLE PRECISION,
                is_halted INTEGER DEFAULT 0,
                halt_reason TEXT,
                trades_today INTEGER DEFAULT 0
            )
        ''')

        # 10. TRADE HISTORY
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trade_history (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20),
                action VARCHAR(20),
                entry_price DOUBLE PRECISION,
                entry_date VARCHAR(50),
                exit_price DOUBLE PRECISION,
                exit_date VARCHAR(50),
                quantity DOUBLE PRECISION,
                strategy VARCHAR(100),
                exit_rule VARCHAR(100),
                pnl DOUBLE PRECISION,
                pnl_pct DOUBLE PRECISION,
                holding_days INTEGER,
                predicted_er DOUBLE PRECISION,
                status VARCHAR(20) DEFAULT 'OPEN'
            )
        ''')

        # 11. STRATEGY PERFORMANCE
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS strategy_performance (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20),
                strategy VARCHAR(100),
                signal_date VARCHAR(50),
                outcome VARCHAR(50),
                pnl DOUBLE PRECISION,
                pnl_pct DOUBLE PRECISION,
                holding_days INTEGER
            )
        ''')

        # 12. AI SELECTOR PREDICTIONS
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ai_selector_predictions (
                ticker VARCHAR(20) PRIMARY KEY,
                recommended_category VARCHAR(100),
                recommended_strategy VARCHAR(100),
                confidence DOUBLE PRECISION,
                features_json TEXT,
                timestamp TIMESTAMP WITH TIME ZONE
            )
        ''')

        # 13. PORTFOLIO RISK
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS portfolio_risk (
                id SERIAL PRIMARY KEY,
                date VARCHAR(20),
                var_95 DOUBLE PRECISION,
                var_99 DOUBLE PRECISION,
                expected_shortfall DOUBLE PRECISION,
                avg_correlation DOUBLE PRECISION,
                max_correlation DOUBLE PRECISION,
                beta DOUBLE PRECISION,
                alpha DOUBLE PRECISION,
                sharpe DOUBLE PRECISION,
                sortino DOUBLE PRECISION,
                max_drawdown DOUBLE PRECISION,
                factor_market DOUBLE PRECISION,
                factor_size DOUBLE PRECISION,
                factor_value DOUBLE PRECISION,
                factor_momentum DOUBLE PRECISION,
                factor_quality DOUBLE PRECISION,
                residual_alpha DOUBLE PRECISION
            )
        ''')

        # 14. PAIRS TRADING
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pairs (
                id SERIAL PRIMARY KEY,
                ticker_a VARCHAR(20),
                ticker_b VARCHAR(20),
                cointegration_pvalue DOUBLE PRECISION,
                hedge_ratio DOUBLE PRECISION,
                half_life DOUBLE PRECISION,
                z_score DOUBLE PRECISION,
                spread_mean DOUBLE PRECISION,
                spread_std DOUBLE PRECISION,
                last_updated TIMESTAMP WITH TIME ZONE,
                status VARCHAR(20) DEFAULT 'ACTIVE'
            )
        ''')

        # 15. HMM REGIME STATES
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS regime_state (
                id INTEGER PRIMARY KEY DEFAULT 1,
                timestamp TIMESTAMP WITH TIME ZONE,
                current_regime VARCHAR(50),
                regime_probability DOUBLE PRECISION,
                bull_prob DOUBLE PRECISION,
                bear_prob DOUBLE PRECISION,
                sideways_prob DOUBLE PRECISION,
                high_vol_prob DOUBLE PRECISION
            )
        ''')

        # 16. RL EXIT MODEL EXPERIENCES
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rl_exit_experiences (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20),
                state_json TEXT,
                action INTEGER,
                reward DOUBLE PRECISION,
                next_state_json TEXT,
                done INTEGER,
                timestamp TIMESTAMP WITH TIME ZONE
            )
        ''')

        conn.commit()
        print("PostgreSQL Database tables created successfully! (V7 — Institutional Grade)")
        
    except Exception as e:
        conn.rollback()
        print(f"Error during PostgreSQL schema build: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

class _DuckDBCursorWrapper:
    """Proxy wrapper to intercept and clean SQL statements dynamically for DuckDB."""
    __slots__ = ('_cursor',)
    def __init__(self, cursor):
        self._cursor = cursor
    def execute(self, query, *args, **kwargs):
        q_clean = query.replace("AUTOINCREMENT", "")
        return self._cursor.execute(q_clean, *args, **kwargs)
    def fetchall(self):
        return self._cursor.fetchall()
    def fetchone(self):
        return self._cursor.fetchone()
    def close(self):
        return self._cursor.close()

def setup_database_sqlite():
    """Fallback standard DuckDB/SQLite schema setup for local backwards compatibility."""
    import sqlite3
    
    # 1. DuckDB Setup
    print(f"Initializing Fallback Local DuckDB Database at: {DUCKDB_PATH}")
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    conn_duck = duckdb.connect(DUCKDB_PATH)
    cursor_duck = _DuckDBCursorWrapper(conn_duck.cursor())

    # 2. SQLite3 Setup
    sqlite_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data', 'trading_brain.db')
    print(f"Initializing standard SQLite3 Database at: {sqlite_path}")
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    conn_sql = sqlite3.connect(sqlite_path)
    cursor_sql = conn_sql.cursor()

    class DualCursor:
        def __init__(self, c_duck, c_sql):
            self.c_duck = c_duck
            self.c_sql = c_sql
        def execute(self, query, *args, **kwargs):
            self.c_duck.execute(query, *args, **kwargs)
            self.c_sql.execute(query, *args, **kwargs)
        def fetchall(self):
            return self.c_duck.fetchall()
        def fetchone(self):
            return self.c_duck.fetchone()
        def close(self):
            self.c_duck.close()
            self.c_sql.close()

    class DualConnection:
        def __init__(self, c_duck, c_sql):
            self.c_duck = c_duck
            self.c_sql = c_sql
        def commit(self):
            self.c_duck.commit()
            self.c_sql.commit()
        def close(self):
            self.c_duck.close()
            self.c_sql.close()

    conn = DualConnection(conn_duck, conn_sql)
    cursor = DualCursor(cursor_duck, cursor_sql)
    
    # 1. Market Data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_data (
            ticker TEXT PRIMARY KEY,
            sector TEXT,
            timestamp TEXT,
            close_price REAL,
            open_price REAL,
            high_price REAL,
            low_price REAL,
            volume REAL,
            garch_volatility REAL,
            volume_ratio REAL,
            vwap REAL,
            hurst_exponent REAL,
            vpin REAL,
            obi REAL,
            micro_price REAL,
            dealer_gex REAL,
            insider_score REAL,
            momentum_12_1 REAL,
            reversal_5d REAL,
            volume_breakout REAL,
            vol_regime REAL,
            trend_strength REAL,
            atr14 REAL,
            volatility_20d REAL
        )
    ''')

    # 2. Alpha Signals
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS alpha_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            ticker TEXT,
            signal_type TEXT,
            target_strategy TEXT,
            confidence REAL,
            exit_rule TEXT,
            status TEXT DEFAULT 'PENDING'
        )
    ''')

    # 3. Execution Queue
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS execution_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            ticker TEXT,
            action TEXT,
            quantity REAL,
            price_estimate REAL,
            order_type TEXT DEFAULT 'MARKET',
            status TEXT DEFAULT 'QUEUED'
        )
    ''')

    # 4. Strategy Weights
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS strategy_weights (
            strategy TEXT PRIMARY KEY,
            weight REAL
        )
    ''')

    strategies_defaults = [
        ('factor_mean_reversion', 0.25),
        ('factor_latent_alpha', 0.25),
        ('factor_microstructure_flow', 0.25),
        ('factor_unified_expected_return', 0.25),
    ]
    for strat, w in strategies_defaults:
        cursor.execute("INSERT OR IGNORE INTO strategy_weights (strategy, weight) VALUES (?, ?)", (strat, w))

    # 5. Strategy Metrics
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS strategy_metrics (
            strategy TEXT PRIMARY KEY,
            win_rate REAL DEFAULT 0.5,
            sharpe_ratio REAL DEFAULT 1.0,
            total_trades INTEGER DEFAULT 0,
            avg_win REAL DEFAULT 0.0,
            avg_loss REAL DEFAULT 0.0,
            max_drawdown REAL DEFAULT 0.0,
            profit_factor REAL DEFAULT 1.0
        )
    ''')

    for strat, _ in strategies_defaults:
        cursor.execute("INSERT OR IGNORE INTO strategy_metrics (strategy) VALUES (?)", (strat,))

    # 6. Daily Watchlist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_watchlist (
            ticker TEXT PRIMARY KEY,
            sector TEXT
        )
    ''')

    # 7. Macro Sentiment
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS macro_sentiment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            vix_level REAL,
            macro_score REAL,
            top_headline TEXT,
            source TEXT DEFAULT 'alpha_vantage'
        )
    ''')

    # 8. Options Queue
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS options_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            underlying TEXT,
            contract_symbol TEXT,
            option_type TEXT,
            action TEXT,
            strike REAL,
            expiry TEXT,
            quantity INTEGER,
            premium_estimate REAL,
            status TEXT DEFAULT 'QUEUED'
        )
    ''')

    # 9. Circuit Breaker
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS circuit_breaker (
            id INTEGER PRIMARY KEY DEFAULT 1,
            date TEXT,
            day_open_value REAL,
            current_value REAL,
            is_halted INTEGER DEFAULT 0,
            halt_reason TEXT,
            trades_today INTEGER DEFAULT 0
        )
    ''')

    # 10. Trade History
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trade_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            action TEXT,
            entry_price REAL,
            entry_date TEXT,
            exit_price REAL,
            exit_date TEXT,
            quantity REAL,
            strategy TEXT,
            exit_rule TEXT,
            pnl REAL,
            pnl_pct REAL,
            holding_days INTEGER,
            predicted_er REAL,
            status TEXT DEFAULT 'OPEN'
        )
    ''')

    # 11. Strategy Performance
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS strategy_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            strategy TEXT,
            signal_date TEXT,
            outcome TEXT,
            pnl REAL,
            pnl_pct REAL,
            holding_days INTEGER
        )
    ''')

    # 12. AI Selector Predictions
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ai_selector_predictions (
            ticker TEXT PRIMARY KEY,
            recommended_category TEXT,
            recommended_strategy TEXT,
            confidence REAL,
            features_json TEXT,
            timestamp TEXT
        )
    ''')

    # 13. Portfolio Risk
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS portfolio_risk (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            var_95 REAL,
            var_99 REAL,
            expected_shortfall REAL,
            avg_correlation REAL,
            max_correlation REAL,
            beta REAL,
            alpha REAL,
            sharpe REAL,
            sortino REAL,
            max_drawdown REAL,
            factor_market REAL,
            factor_size REAL,
            factor_value REAL,
            factor_momentum REAL,
            factor_quality REAL,
            residual_alpha REAL
        )
    ''')

    # 14. Pairs Trading
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pairs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker_a TEXT,
            ticker_b TEXT,
            cointegration_pvalue REAL,
            hedge_ratio REAL,
            half_life REAL,
            z_score REAL,
            spread_mean REAL,
            spread_std REAL,
            last_updated TEXT,
            status TEXT DEFAULT 'ACTIVE'
        )
    ''')

    # 15. HMM Regime States
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS regime_state (
            id INTEGER PRIMARY KEY DEFAULT 1,
            timestamp TEXT,
            current_regime TEXT,
            regime_probability REAL,
            bull_prob REAL,
            bear_prob REAL,
            sideways_prob REAL,
            high_vol_prob REAL
        )
    ''')

    # 16. RL Exit Experiences
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rl_exit_experiences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            state_json TEXT,
            action INTEGER,
            reward REAL,
            next_state_json TEXT,
            done INTEGER,
            timestamp TEXT
        )
    ''')

    # Run V13 real predictive features migration on SQLite and DuckDB
    new_cols_lt = ['momentum_12_1', 'reversal_5d', 'volume_breakout', 'vol_regime', 'trend_strength', 'atr14', 'volatility_20d']
    for col in new_cols_lt:
        # SQLite migration
        try:
            conn_sql.execute(f"ALTER TABLE market_data ADD COLUMN {col} REAL")
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower() and "already exists" not in str(e).lower():
                print(f"SQLite migration warning on {col}: {e}")
        
        # DuckDB migration
        try:
            conn_duck.execute(f"ALTER TABLE market_data ADD COLUMN {col} DOUBLE")
        except Exception as e:
            if "duplicate column" not in str(e).lower() and "already exists" not in str(e).lower():
                print(f"DuckDB migration warning on {col}: {e}")

    conn.commit()
    conn.close()
    print("Fallback DuckDB Database tables created successfully!")

def setup_database():
    # Always ensure local SQLite sandbox is fully initialized for regression tests
    setup_database_sqlite()
    
    # Also setup PostgreSQL if active
    if os.getenv("DB_HOST"):
        try:
            setup_database_postgres()
        except Exception as e:
            print(f"PostgreSQL setup failed ({e})")

if __name__ == '__main__':
    setup_database()

