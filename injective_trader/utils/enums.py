from enum import Enum, auto


class MarketType(Enum):
    SPOT = 1
    DERIVATIVE = 2
    BINARY = 3


class Side(Enum):
    BUY = 1
    SELL = 2


class OrderStatus(Enum):
    BOOKED = "Booked"
    PARTIAL_FILLED = "PartialFilled"
    FILLED = "Filled"
    CANCELLED = "Cancelled"
    EXPIRED = "Expired"


class OrderType(Enum):
    LIMIT = 1
    MARKET = 2
    CONDITION = 3

class Exchange(Enum):
    HELIX = "helix"
    BINANCE = "binance"

class OrderedEnum(Enum):
    def __ge__(self, other):
        if self.__class__ is other.__class__:
            return self.value >= other.value
        return NotImplemented

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self.value > other.value
        return NotImplemented

    def __le__(self, other):
        if self.__class__ is other.__class__:
            return self.value <= other.value
        return NotImplemented

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


class MessageType(OrderedEnum):
    BANK_BALANCE = 0
    SUBACCOUNT_DEPOSIT = 1
    SPOT_ORDER = 2
    DERIVATIVE_ORDER = 3
    POSITION = 4
    ORACLE_PRICE = 5
    SPOT_TRADE = 6
    DERIVATIVE_TRADE = 7


class TraderType(OrderedEnum):
    MAKER = 0
    TAKER = 1
    UNKNOWN = 2


class Event(OrderedEnum):
    URGENT = auto()
    RISK_UPDATE = auto()
    STRATEGY_EXECUTION = auto()
    DATA_REQUEST = auto()
    STRATEGY_UPDATE = auto()
    ORACLE_UPDATE = auto()
    DERIVATIVE_TRADE_UPDATE = auto()
    SPOT_TRADE_UPDATE = auto()
    DERIVATIVE_ORDER_UPDATE = auto()
    SPOT_ORDER_UPDATE = auto()
    POSITION_UPDATE = auto()
    DEPOSIT_UPDATE = auto()
    BALANCE_UPDATE = auto()
    ACCOUNT_INIT = auto()
    ORDERBOOK_UPDATE = auto()
    MARKET_INIT = auto()
    BROADCAST = auto()
    CONFIG_UPDATE = auto()
    EXTERNAL_INFO = auto()
    LIQUIDATION_INFO = auto()
    UNKNOWN = auto()

class UpdateType(Enum):
    # Market Data Updates
    OnOrderbook = "orderbook_update"
    OnOraclePrice = "oracle_update"
    OnExternalInfo = "external_info"

    # Account Updates
    OnBankBalance = "bank_balance_update"
    OnDeposit = "deposit_update"
    OnSpotOrder = "spot_order_update"
    OnDerivativeOrder = "derivative_order_update"
    OnSpotTrade = "spot_trade_update"
    OnDerivativeTrade = "derivative_trade_update"
    OnPosition = "position_update"

    # Risk Alerts
    OnMarginWarning = "margin_warning"
    OnDrawdownAlert = "drawdown_alert"
    OnLiquidationRisk = "liquidation_risk"
    OnBalanceThreshold = "balance_threshold"

    OnLiquidatablePosition = "liquidatable_position_update"

class AlertSeverity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"

class AlertType(Enum):
    DRAWDOWN = auto()
    MARGIN = auto()
    LIQUIDATION = auto()
    BALANCE = auto()
    PERFORMANCE = auto()
    SYSTEM = auto()
