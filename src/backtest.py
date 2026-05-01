import backtrader as bt
import pandas as pd
import yfinance as yf
import datetime

class MACDStrategy(bt.Strategy):
    """
    Backtrader implementation of the MACD Strategy.
    """
    params = (
        ('macd1', 12),
        ('macd2', 26),
        ('macdsig', 9),
    )

    def __init__(self):
        self.order = None
        
        # Calculate MACD
        self.macd = bt.indicators.MACD(
            self.data.close,
            period_me1=self.params.macd1,
            period_me2=self.params.macd2,
            period_signal=self.params.macdsig
        )
        
        # CrossOver signal: MACD line crosses above/below Signal line
        self.crossover = bt.indicators.CrossOver(self.macd.macd, self.macd.signal)

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.data.datetime[0]
        if isinstance(dt, float):
            dt = bt.num2date(dt)
        print(f'{dt.isoformat()} - {txt}')

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'BUY EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.2f}')
            elif order.issell():
                self.log(f'SELL EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.2f}')
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log(f'OPERATION PROFIT, GROSS {trade.pnl:.2f}, NET {trade.pnlcomm:.2f}')

    def next(self):
        if self.order:
            return # Wait for pending order to resolve

        # Check if we are in the market
        if not self.position:
            # Not in the market, we can buy
            if self.crossover > 0:
                self.log(f'BUY SIGNAL (MACD > Signal), Price: {self.data.close[0]:.2f}')
                # Buy as much as we can afford with paper money
                self.order = self.buy()
        else:
            # We are in the market, we can sell
            if self.crossover < 0:
                self.log(f'SELL SIGNAL (MACD < Signal), Price: {self.data.close[0]:.2f}')
                # Sell (close position)
                self.order = self.sell()


def fetch_stock_data(symbol='COST', start='2020-01-01', end='2026-04-30'):
    print(f"Fetching daily data for {symbol} from Yahoo Finance...")
    
    df = yf.download(symbol, start=start, end=end)
    
    if isinstance(df.columns, pd.MultiIndex):
        # If downloaded using yfinance's multi-level columns, flatten them
        df.columns = df.columns.get_level_values(0)
    
    # Ensure columns match backtrader expectations
    df.rename(columns={
        'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'
    }, inplace=True)
    
    print("Data fetched successfully!")
    return df

def run_backtest():
    cerebro = bt.Cerebro()

    # Add the strategy
    cerebro.addstrategy(MACDStrategy)

    # Fetch Data
    df = fetch_stock_data(symbol='COST', start='2020-01-01', end='2026-04-30')
    data = bt.feeds.PandasData(dataname=df)
    cerebro.adddata(data)

    # Set paper money broker settings
    STARTING_CASH = 10000.0
    cerebro.broker.setcash(STARTING_CASH)
    
    # Set commission (e.g., Binance standard 0.1%)
    cerebro.broker.setcommission(commission=0.001)

    # Use a sizer to buy using 95% of available cash (so we can afford fractional BTC)
    cerebro.addsizer(bt.sizers.PercentSizer, percents=95)

    # Add Analyzers
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="trade_analyzer")
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")

    print(f'\nStarting Portfolio Value: ${cerebro.broker.getvalue():.2f}')

    # Run the backtest
    results = cerebro.run()
    strat = results[0]

    final_value = cerebro.broker.getvalue()
    print(f'\nFinal Portfolio Value: ${final_value:.2f}')
    print(f'Net Profit/Loss: ${(final_value - STARTING_CASH):.2f}')

    # Extract Trade Analyzer results
    trade_analysis = strat.analyzers.trade_analyzer.get_analysis()
    
    total_trades = trade_analysis.get('total', {}).get('closed', 0)
    if total_trades > 0:
        won_trades = trade_analysis.get('won', {}).get('total', 0)
        lost_trades = trade_analysis.get('lost', {}).get('total', 0)
        win_rate = (won_trades / total_trades) * 100
        
        print(f"\n--- Backtest Statistics ---")
        print(f"Total Closed Trades : {total_trades}")
        print(f"Winning Trades      : {won_trades}")
        print(f"Losing Trades       : {lost_trades}")
        print(f"Win Rate            : {win_rate:.2f}%")
        
        drawdown = strat.analyzers.drawdown.get_analysis()
        print(f"Max Drawdown        : {drawdown.max.drawdown:.2f}%")
    else:
        print("\nNo trades were closed during the backtest period.")

if __name__ == '__main__':
    run_backtest()
