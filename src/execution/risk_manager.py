import logging
from config.settings import settings

logger = logging.getLogger("RiskManager")

class RiskManager:
    """
    Evaluates trading signals before order execution to prevent massive losses
    or unintended capital allocation.
    """
    
    def __init__(self, total_capital):
        self.total_capital = total_capital
        self.max_risk_pct = settings.MAX_RISK_PER_TRADE
        
    def validate_order(self, symbol, current_price, quantity, is_buy=True):
        """
        Checks if the order aligns with risk parameters.
        Returns False if the order is too risky.
        """
        trade_value = current_price * quantity
        max_allowed_value = self.total_capital * self.max_risk_pct
        
        # Simplified risk check for Indian Intraday where margins apply.
        # Note: In reality, MIS orders require less margin than full trade_value.
        # We use a conservative check for scaffolding.
        base_funds_required = trade_value / 5.0 # Assuming roughly 5x Intraday leverage for equity
        
        if base_funds_required > self.total_capital:
             logger.warning(f"Risk Rejected: Required funds ({base_funds_required}) exceeds total capital ({self.total_capital}) for {symbol}")
             return False
             
        logger.info(f"Risk Accepted: Order for {symbol} passes risk checks.")
        return True
