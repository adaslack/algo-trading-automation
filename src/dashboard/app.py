from flask import Flask, render_template, jsonify
import sqlite3
import os

app = Flask(__name__)
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data', 'trading_brain.db')

def get_db_connection():
    conn = sqlite3.connect(DB_PATH, timeout=5.0)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # 1. Get Regime
        c.execute('SELECT current_regime, bull_prob, bear_prob, high_vol_prob FROM regime_state ORDER BY id DESC LIMIT 1')
        regime_row = c.fetchone()
        regime = dict(regime_row) if regime_row else {'current_regime': 'Unknown', 'bull_prob': 0, 'bear_prob': 0, 'high_vol_prob': 0}
        
        # 2. Get Open Positions
        c.execute('SELECT ticker, entry_price, entry_date, quantity, pnl_pct, holding_days, strategy FROM trade_history WHERE status="OPEN"')
        positions = [dict(row) for row in c.fetchall()]
        
        # 3. Get Risk & Factor Model
        c.execute('''
            SELECT var_95, factor_market, factor_size, factor_value, factor_momentum, factor_quality, residual_alpha 
            FROM portfolio_risk ORDER BY id DESC LIMIT 1
        ''')
        risk_row = c.fetchone()
        risk = dict(risk_row) if risk_row else {}
        
        # 4. Get Strategy Weights
        c.execute('SELECT strategy, weight FROM strategy_weights ORDER BY weight DESC')
        weights = [dict(row) for row in c.fetchall()]
        
        # 5. Get Pairs
        c.execute('SELECT ticker_a, ticker_b, z_score FROM pairs WHERE ABS(z_score) > 1.5')
        pairs = [dict(row) for row in c.fetchall()]
        
        conn.close()
        
        return jsonify({
            'status': 'success',
            'regime': regime,
            'positions': positions,
            'risk': risk,
            'weights': weights,
            'pairs': pairs
        })
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    app.run(debug=True, port=5000)
