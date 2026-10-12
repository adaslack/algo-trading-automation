from abc import ABC, abstractmethod
from typing import Optional
from dataclasses import dataclass, field
from enum import Enum


class OrderSideEnum(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderTypeEnum(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"


class TimeInForceEnum(str, Enum):
    DAY = "DAY"
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"


@dataclass
class BrokerQuote:
    """Standardized quote from any broker."""
    symbol: str
    bid_price: float = 0.0
    ask_price: float = 0.0
    mid_price: float = 0.0
    bid_size: int = 0
    ask_size: int = 0
    last_price: float = 0.0

    def __post_init__(self):
        if self.mid_price == 0.0 and self.bid_price > 0 and self.ask_price > 0:
            self.mid_price = (self.bid_price + self.ask_price) / 2.0


@dataclass
class BrokerAccount:
    """Standardized account info from any broker."""
    portfolio_value: float = 0.0
    buying_power: float = 0.0
    cash: float = 0.0
    currency: str = "USD"


@dataclass
class BrokerPosition:
    """Standardized position from any broker."""
    symbol: str
    qty: float = 0.0
    avg_cost: float = 0.0
    market_value: float = 0.0
    unrealized_pnl: float = 0.0
    side: str = "long"  # "long" or "short"


@dataclass
class BrokerOrder:
    """Standardized order submission request."""
    symbol: str
    qty: float
    side: OrderSideEnum
    order_type: OrderTypeEnum = OrderTypeEnum.MARKET
    time_in_force: TimeInForceEnum = TimeInForceEnum.DAY
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None


@dataclass
class BrokerOrderResult:
    """Standardized order result from any broker."""
    order_id: str = ""
    symbol: str = ""
    filled_qty: float = 0.0
    filled_avg_price: float = 0.0
    status: str = "submitted"
    commission: float = 0.0


@dataclass
class BrokerBar:
    """Standardized OHLCV bar."""
    timestamp: object = None
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0


@dataclass
class BrokerAsset:
    """Standardized asset info."""
    symbol: str
    name: str = ""
    exchange: str = ""
    asset_class: str = "us_equity"
    tradable: bool = True
    marginable: bool = False
    fractionable: bool = False


class BaseBroker(ABC):
    """
    Abstract Base Class for broker integrations.
    Provides a unified interface for active and future brokers.
    All implementations must follow this interface.
    """

    @abstractmethod
    def connect(self) -> bool:
        """Establish connection / session with the broker. Returns True on success."""
        pass

    @abstractmethod
    def disconnect(self):
        """Cleanly disconnect from the broker."""
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """Check if broker connection is active."""
        pass

    # ── Account & Positions ───────────────────────────────────────────────────

    @abstractmethod
    def get_account(self) -> BrokerAccount:
        """Retrieve account summary (portfolio value, buying power, cash)."""
        pass

    @abstractmethod
    def get_positions(self) -> dict[str, BrokerPosition]:
        """Get all current positions. Returns {symbol: BrokerPosition}."""
        pass

    @abstractmethod
    def get_funds(self) -> float:
        """Retrieve available trading capital (buying power)."""
        pass

    # ── Market Data ───────────────────────────────────────────────────────────

    @abstractmethod
    def get_latest_quote(self, symbol: str) -> BrokerQuote:
        """Get the latest bid/ask/mid quote for a symbol."""
        pass

    @abstractmethod
    def get_historical_bars(self, symbols: list[str], timeframe: str,
                            start: object, end: object) -> dict[str, list[BrokerBar]]:
        """
        Fetch historical OHLCV bars.
        timeframe: '1Min', '5Min', '1Hour', '1Day'
        Returns {symbol: [BrokerBar, ...]}
        """
        pass

    @abstractmethod
    def get_tradable_assets(self) -> list[BrokerAsset]:
        """Get list of all tradable US equity assets."""
        pass

    # ── Market Status ─────────────────────────────────────────────────────────

    @abstractmethod
    def is_market_open(self) -> bool:
        """Check if the US stock market is currently open for trading."""
        pass

    # ── Order Execution ───────────────────────────────────────────────────────

    @abstractmethod
    def place_order(self, order: BrokerOrder) -> BrokerOrderResult:
        """Submit an order to the broker. Returns the order result."""
        pass

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order. Returns True on success."""
        pass

    # ── Real-Time Streaming ───────────────────────────────────────────────────

    @abstractmethod
    def subscribe_bars(self, symbols: list[str], callback) -> None:
        """
        Subscribe to real-time minute bars for the given symbols.
        callback(symbol, bar: BrokerBar) is called on each new bar.
        """
        pass

    @abstractmethod
    def run_stream(self) -> None:
        """Start the real-time data stream (blocking call)."""
        pass

    # ── Broker-Specific Properties ────────────────────────────────────────────

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable broker name."""
        pass

    @property
    @abstractmethod
    def supports_fractional(self) -> bool:
        """Whether this broker supports fractional share orders."""
        pass

    @property
    @abstractmethod
    def commission_per_share(self) -> float:
        """Estimated commission per share (0.0 for commission-free brokers)."""
        pass
