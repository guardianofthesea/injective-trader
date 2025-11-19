from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Dict
from datetime import datetime, timedelta

from injective_trader.utils.enums import AlertType, AlertSeverity, Side
from injective_trader.strategy.performance import PerformanceMetrics, Alert
from injective_trader.domain.market.market import Market
from injective_trader.domain.account.account import BankBalance
from injective_trader.domain.account.subaccount import Deposit
from injective_trader.domain.account.order import Order
from injective_trader.domain.account.position import Position


class UpdateHandler(ABC):
    """Interface for strategy update handling"""

    def __init__(self, logger, config, metrics: PerformanceMetrics, strategy):
        """
        Initialize handler with dependencies

        Args:
            logger: Logger instance
            config: Strategy configuration
            metrics: Optional PerformanceMetrics instance
        """
        self.logger = logger
        self.config = config
        self.metrics = metrics
        self.strategy = strategy

    async def process(self, update_data) -> Dict|None:
        """
        Process update data
        Returns None if data should be ignored
        """

        try:
            return await self._process_update(update_data)
        except Exception as e:
            self.logger.error(f"Error processing update in {self.__class__.__name__}: {e}")
            return None

    @abstractmethod
    async def _process_update(self, update_data) -> Dict:
        """Process specific update type. To be implemented by subclasses."""
        pass


class OrderbookHandler(UpdateHandler):
    """
    Handles orderbook updates (UpdateType.OnOrderbook)

    update_data contains:
    - market_id: str
    - market: Market object with updated orderbook
    - sequence: int
    - block_time: Optional[str]
    """

    async def _process_update(self, update_data: Dict) -> Dict:
        """Process orderbook update."""
        self.logger.debug(f"{self.__class__.__name__} update date: {update_data}")
        market: Market = update_data["market"]

        # Validate orderbook
        bid, ask = market.orderbook.tob()
        if bid and ask and bid >= ask:
            self.logger.warning(f"Crossed orderbook detected in {market.market_id}")

        return update_data

class OracleHandler(UpdateHandler):
    """Handler for oracle price updates"""
    async def _process_update(self, update_data: Dict) -> Dict:
        self.logger.debug(f"{self.__class__.__name__} update date: {update_data}")
        return update_data

class BalanceHandler(UpdateHandler):
    """
    Handles balance updates (UpdateType.OnBankBalance)

    update_data contains:
    - account_address: str
    - account: Account object
    - balance: BankBalance object
    """

    async def _process_update(self, update_data: Dict) -> Dict:
        self.logger.debug(f"{self.__class__.__name__} update date: {update_data}")
        balance: BankBalance = update_data["balance"]

        # Could track equity changes here
        if balance.amount > 0:
            self.metrics.highest_equity = max(
                self.metrics.highest_equity,
                balance.amount
            )

        return update_data

class DepositHandler(UpdateHandler):
    """
    Handles deposit updates (UpdateType.OnDeposit)

    update_data contains:
    - account_address: str
    - subaccount_id: str
    - account: Account object
    - deposit: Deposit object
    """

    async def _process_update(self, update_data: Dict) -> Dict:
        self.logger.debug(f"{self.__class__.__name__} update date: {update_data}")
        deposit: Deposit = update_data["deposit"]

        # Track margin usage
        if deposit.total_balance > 0:
            usage_ratio = (deposit.total_balance - deposit.available_balance) / deposit.total_balance
            if usage_ratio > self.metrics.margin_warning:
                self.metrics.add_alert(Alert(
                    AlertType.MARGIN,
                    AlertSeverity.WARNING,
                    f"High deposit usage: {float(usage_ratio * 100):.2f}"
                ))

        return update_data

class PositionHandler(UpdateHandler):
    """
    Handles position updates (UpdateType.OnPosition)

    update_data contains:
    - market_id: str
    - account_address: str
    - subaccount_id: str
    - account: Account object
    - position: Position object
    """

    async def _process_update(self, update_data: Dict) -> Dict:
        self.logger.debug(f"{self.__class__.__name__} update date: {update_data}")

        position: None|Position = update_data.get("position", None)
        if not position:
            self.logger.error("Missing position in update")
            return update_data

        # Process position risks if metrics available
        if self.metrics and hasattr(self.metrics, "process_position_update"):
            self.metrics.process_position_update(position)

        # Check if position is closing
        if position.quantity == 0 and self.metrics and hasattr(self.metrics, "add_closed_position"):
            self.metrics.add_closed_position(position.to_dict())

        return update_data


class OrderHandler(UpdateHandler):
    """
    Handles order updates (UpdateType.OnSpotOrder or OnDerivativeOrder)

    update_data contains:
    - market_id: str
    - subaccount_id: str
    - account: Account object
    - order: Order object
    """
    async def _process_update(self, update_data: Dict) -> Dict:
        self.logger.debug(f"{self.__class__.__name__} update date: {update_data}")
        order: Order = update_data["order"]
        #market: Market = update_data["market"]

        # Track order volume
        self.metrics.total_volume += order.price * order.quantity

        return update_data

class TradeHandler(UpdateHandler):
    """
    Handles trade updates (UpdateType.OnSpotTrade or OnDerivativeTrade)

    update_data contains:
    - market_id: str
    - subaccount_id: str
    - account: Account object
    - trade: Order object representing the trade
    - order: Original Order object that was filled
    """

    async def _process_update(self, update_data: Dict) -> Dict:
        """Process trade update."""
        self.logger.debug(f"{self.__class__.__name__} update date: {update_data}")

        # Update trade metrics
        alerts = self.metrics.process_trade(
            update_data["trade"],
            update_data["order"]
        )

        return update_data


class ExternalInfoHandler(UpdateHandler):
    """
    Handles trade updates (UpdateType.OnSpotTrade or OnDerivativeTrade)

    update_data contains:
    - market_id: str
    - symbol: str, redis channel name
    - type: str, redis data type
    - metrics: dict, Decimals → str for JSON safety
    - changed: dict{"prev": str, "curr": str, "pct": str}
    - Other external infos defined by user
    """

    async def _process_update(self, update_data: Dict) -> Dict:
        self.logger.debug(f"{self.__class__.__name__} update date: {update_data}")
        return update_data

class LiquidationHandler(UpdateHandler):
    """
    Handles liquidation updates (UpdateType.OnLiquidation)

    update_data contains:
    - market_id: str
    - ticker: str
    - subaccount_id: str
    - direction: str
    - quantity: Decimal
    - aggregate_reduce_only_quantity: Decimal
    - entry_price: Decimal
    - margin: Decimal
    - liquidation_price: Decimal
    - mark_price: Decimal
    """

    async def _process_update(self, update_data: Dict) -> Dict:
        self.logger.debug(f"{self.__class__.__name__} update date: {update_data}")
        return update_data