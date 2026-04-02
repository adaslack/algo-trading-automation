import logging
from src.brokers.base_broker import BaseBroker
from src.execution.risk_manager import RiskManager

logger = logging.getLogger("OrderRouter")

class OrderRouter:
    """
    The middle-man between the Strategy and the Broker.
    It takes a signal, consults Risk Management, and then commands the Broker.
    """
    
    def __init__(self, broker: BaseBroker, risk_manager: RiskManager):
        self.broker = broker
        self.risk_manager = risk_manager
        
    def process_signal(self, symbol, signal, current_price):
        """
        Processes a numerical signal (1=Buy, -1=Sell, 0=Hold)
        and translates it into an actual broker order if risk allows.
        """
        if signal == 0:
            return None # Hold
            
        # Simplified logic: Hardcoding 1 quantity for scaffolding.
        # In production this would be calculated by RiskManager based on capital/stoploss.
        trade_quantity = 1 
        is_buy = signal == 1
        
        # 1. Ask Risk Manager
        if self.risk_manager.validate_order(symbol, current_price, trade_quantity, is_buy):
            # 2. Command Broker
            action = "BUY" if is_buy else "SELL"
            logger.info(f"Routing {action} order for {trade_quantity}x {symbol} @ {current_price}")
            
            try:
                # Assuming Market Order
                order_response = self.broker.place_order(
                    symbol=symbol,
                    current_price=current_price,
                    quantity=trade_quantity,
                    order_type="MARKET",
                    is_buy=is_buy
                )
                return order_response
            except Exception as e:
                logger.error(f"Failed to route order to broker: {e}")
                return None
        else:
             logger.warning("Order Router aborted trade due to Risk Manager rejection.")
             return None
