from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Tuple, Type
from decimal import Decimal
from dataclasses import dataclass, field
from datetime import datetime
import uuid

from injective_trader.domain.account.account import Account
from injective_trader.domain.account.order import Order
from injective_trader.domain.account.position import Position
from injective_trader.domain.market.market import Market
from injective_trader.utils.enums import UpdateType
from injective_trader.strategy.handlers import (
    UpdateHandler, OrderbookHandler, OracleHandler, DepositHandler,
    PositionHandler, BalanceHandler, TradeHandler, OrderHandler
)
from injective_trader.strategy.performance import PerformanceMetrics, AlertType, AlertSeverity


@dataclass
class StrategyResult:
    """Standardized strategy execution result"""
    orders: List[Order] = field(default_factory=list)
    cancellations: List[Dict] = field(default_factory=list)  # Order hashes to cancel
    margin_updates: List[Dict] = field(default_factory=list)
    liquidations: List[Dict] = field(default_factory=list)

class Strategy(ABC):
    """
    Base Strategy class for implementing trading strategies.

    Required implementations:
    1. _create_update_handler(): Create strategy-specific update handler
    2. on_initialize(): Initialize strategy-specific state and parameters

    Optional overrides:
    1. is_applicable(): Define when strategy should be active
    2. round_to_tick(): Custom tick size rounding if needed

    Example implementation:
    ```python
    class MyStrategy(Strategy):
        def _create_update_handler(self) -> UpdateHandler:
            return MyStrategyHandler(self.logger, self.config)

        def on_initialize(self, accounts: Dict[str, Account], markets: Dict[str, Market]):
            # Initialize strategy parameters
            self.param1 = self.config.get("Parameters", {}).get("param1", default_value)
            self.param2 = self.config.get("Parameters", {}).get("param2", default_value)

        def is_applicable(self, accounts: Dict[str, Account], markets: Dict[str, Market]) -> bool:
            # Define when strategy should run
            return all(condition for condition in my_conditions)
    ```

    Configuration example:
    ```yaml
    MyStrategy:
      Name: "MyStrategy"
      Class: "MyStrategy"
      MarketIds:
        - "0xmarket1"
        - "0xmarket2"
      AccountAddresses:
        - "inj1..."
      Parameters:
        param1: value1
        param2: value2
      RiskConfig:
        DrawdownWarning: 0.1
        DrawdownCritical: 0.2
        MarginWarning: 0.7
        MarginCritical: 0.8
    ```
    """

    def __init__(self, logger, config):
        """
        Initialize strategy with logger and configuration.

        Args:
            logger: Logger instance for strategy logging
            config: Strategy configuration dictionary
                Required keys:
                - MarketIds: List of market IDs to trade
                - AccountAddresses: List of account addresses to use
                Optional keys:
                - Name: Strategy name
                - Parameters: Strategy-specific parameters
                - RiskConfig: Risk management parameters
        """
        self.logger = logger
        self.config = config
        self.name = config.get("Name", "")
        self.logger.debug(f"Initialize in base strategy...")

        # strategy configurations
        if not config.get("MarketIds"):
            raise ValueError("MarketIds must be provided in strategy configuration")
        self.market_ids = config["MarketIds"]
        self.account_addresses = config.get("AccountAddresses", "") # Support strategy without transaction
        self.subaccount_ids = config.get("SubaccountIds", []) # Optional
        self.risk = config.get("Risk", None) # Optional
        self.fee_recipient = config.get("FeeRecipient", "")
        self.cid_prefix = config.get("CIDPrefix", self.name)
        self._order_counter = 0

        self.logger.debug(f"In strategy {self.name}, initialize markets {self.market_ids} and accounts {self.account_addresses}")

        # Validate trading configuration for this strategy
        if "TradingAccount" in config:
            self.trading_mode = "direct"
            self.trading_account = config["TradingAccount"]
        elif "Granter" in config and "Grantees" in config:
            self.trading_mode = "authz"
            self.granter = config["Granter"]
            self.grantees = config["Grantees"]
        else:
            self.trading_mode = None
            self.logger.warning("Neither TradingAccount or Granter/Grantees can be found. "
                                "Must specify either TradingAccount or Granter/Grantees in strategy configuration")

        # Initilize performance metrics
        self.logger.debug(f"Initialize metrics...")
        self.metrics = PerformanceMetrics(logger, datetime.now())
        risk_config = config.get("RiskConfig", {})
        self.metrics.set_risk_thresholds(
            drawdown_warning=float(risk_config.get("DrawdownWarning", 0.1)),
            drawdown_critical=float(risk_config.get("DrawdownCritical", 0.2)),
            margin_warning=float(risk_config.get("MarginWarning", 0.7)),
            margin_critical=float(risk_config.get("MarginCritical", 0.8))
        )

        # Initialize handlers
        self.logger.debug("creating handlers")
        self.handlers = self._create_handlers()

        # State management
        self.markets: Dict[str, Market] = {}
        self.accounts: Dict[str, Account] = {}
        self.initialized = False

        self.current_positions: Dict[Tuple[str, str], Position] = {} # (subaccount_id, market_id) -> position
        self.price_history: Dict[str, List[Decimal]] = {}
        self._last_orderbook_update: Dict[str, float] = {}

        # Link positions to metrics - this is key for PnL tracking
        self.logger.debug("setting position reference")
        self.metrics.set_positions_reference(self.current_positions)
        self.logger.debug("position reference set")

    def _create_handler(self, handler_class: Type[UpdateHandler]) -> UpdateHandler:
        """Create handler instance with strategy config."""
        return handler_class(self.logger, self.config, self.metrics, self)

    def _create_handlers(self) -> dict[UpdateType, UpdateHandler]:
        """Create handler instance with strategy config."""
        handler_classes = {
            UpdateType.OnOrderbook: OrderbookHandler,
            UpdateType.OnOraclePrice: OracleHandler,
            UpdateType.OnBankBalance: BalanceHandler,
            UpdateType.OnDeposit: DepositHandler,
            UpdateType.OnPosition: PositionHandler,
            UpdateType.OnSpotTrade: TradeHandler,
            UpdateType.OnDerivativeTrade: TradeHandler,
            UpdateType.OnSpotOrder: OrderHandler,
            UpdateType.OnDerivativeOrder: OrderHandler,
        }

        handlers = {}
        for update_type, handler_class in handler_classes.items():
            handler = handler_class(self.logger, self.config, self.metrics, self)
            handlers[update_type] = handler

        return handlers

    def _generate_cid(self) -> str:
        """Generate unique client order ID using UUID (max 36 chars)"""
        return str(uuid.uuid4())

    @abstractmethod
    def on_initialize(self, accounts: Dict[str, Account], markets: Dict[str, Market]) -> StrategyResult | None:
        """
        Initialize strategy-specific state and parameters.
        Called once before strategy starts processing updates.

        Args:
            accounts: Dictionary of account_address -> Account
            markets: Dictionary of market_id -> Market

        Example:
        ```python
        def on_initialize(self, accounts: Dict[str, Account], markets: Dict[str, Market]):
            # Get strategy parameters from config
            params = self.config.get("Parameters", {})
            self.trade_size = Decimal(str(params.get("TradeSize", "1.0")))
            self.max_position = Decimal(str(params.get("MaxPosition", "10.0")))

            # Initialize strategy state
            self.current_positions = {}
            self.last_trade_prices = {}

            # Set up technical indicators
            self.sma_period = params.get("SMAPeriod", 20)
            self.price_history = {market_id: [] for market_id in self.market_ids}
        ```
        """
        pass


    def initialize(self, accounts: Dict[str, Account], markets: Dict[str, Market]):
        """
        Initialize strategy and its components.

        Args:
            accounts: Dictionary of account_address -> Account
            markets: Dictionary of market_id -> Market

        Raises:
            ValueError: If required markets or accounts are not found
        """
        try:
            # Validate configuration
            if accounts:
                self._validate_config(accounts, markets)
                self.logger.debug(f"Configuration for strategy {self.name} is validated.")
            else:
                self.logger.warning("accounts is empty")

            # Initialize current positions and balances
            self._initialize_state(accounts, markets)
            self.logger.info(f"Current positions and balances of strategy {self.name} has been initialized.")

            # Call strategy-specific initialization
            result = self.on_initialize(accounts, markets)

            self.initialized = True
            self.logger.info(f"""
            Strategy {self.name} initialized:
            Markets: {len(self.market_ids)}
            Accounts: {len(self.account_addresses)}
            Handler: {[h.__class__.__name__ for h in self.handlers.values()]}
            """)

            return result

        except Exception as e:
            self.logger.error(f"Error initializing strategy: {e}")
            raise

    def _validate_config(self, accounts: Dict[str, Account], markets: Dict[str, Market]):
        """Validate required markets and accounts exist"""
        # Market validation
        for market_id in self.market_ids:
            if market_id not in markets:
                #self.logger.warning(f"Configured market {market_id} not found in available markets")
                raise ValueError(f"Configured market {market_id} not found in available markets")

        # Account validation
        for account_address in self.account_addresses:
            if account_address not in accounts:
                self.logger.warning(f"Configured account {account_address} not found in available accounts")
                #raise ValueError(f"Configured account {account_address} not found in available accounts")

        if self.subaccount_ids:
            valid_subaccounts = set()
            for account_address in self.account_addresses:
                valid_subaccounts.update(accounts[account_address].subaccounts.keys())

            for sub_id in self.subaccount_ids:
                if sub_id not in valid_subaccounts:
                    self.logger.warning(f"Configured subaccount {sub_id} not found in configured accounts")
                    #raise ValueError(f"Configured subaccount {sub_id} not found in configured accounts")

    def _initialize_state(self, accounts: Dict[str, Account], markets: Dict[str, Market]):
        """Initialize strategy state from current market/account data"""
        # Initialize positions
        self.current_positions.clear()

        for account_address in self.account_addresses:
            account = accounts[account_address]
            self.accounts[account_address] = accounts[account_address]

            # Get filtered subaccounts
            subaccounts = (
                (sub_id, sub_acc)
                for sub_id, sub_acc in account.subaccounts.items()
                if not self.subaccount_ids or sub_id in self.subaccount_ids
            )

            for subaccount_id, subaccount in subaccounts:
                for market_id, position in subaccount.positions.items():
                    if market_id not in self.market_ids:
                        continue

                    if position.quantity != Decimal("0"):
                        key = (subaccount_id, market_id)
                        self.current_positions[key] =  position
                        #{
                        #    "quantity": position.quantity,
                        #    "entry_price": position.entry_price,
                        #    "is_long": position.is_long,
                        #    "margin": position.margin
                        #}

        # Initialize price history
        for market_id in self.market_ids:
            self.price_history[market_id] = []
            self.markets[market_id] = markets[market_id]

    async def execute(
        self,
        accounts: Dict[str, Account],
        markets: Dict[str, Market],
        update_type: UpdateType,
        update_data: Dict
    ) -> StrategyResult | None:
        """
        Execute strategy based on market updates.

        Args:
            accounts: Dictionary of account_address -> Account
            markets: Dictionary of market_id -> Market
            update_type: Type of update being processed
            update_data: Update-specific data dictionary
                Common fields:
                - market_id: Market identifier
                - account_address: Account address (for account updates)
                - subaccount_id: Subaccount identifier (for position/trade updates)

        Returns:
            StrategyResult:
            - orders
            - cancellations
            - marginupdates
        """
        if not self.initialized:
            self.initialize(accounts, markets)

        try:
            # Update state
            self.accounts = accounts
            self.markets = markets

            # Get handler for update type
            self.logger.debug(f"getting handler for {update_type}")
            handler = self.handlers.get(update_type)
            if not handler:
                self.logger.warning(f"Update type {update_type} does not have specific handlers in strategy {self.name}")
                return None

            # Process update
            self.logger.debug(f"processing update data {update_data}")
            processed_data = await handler.process(update_data)
            if processed_data and "Status" in processed_data:
                return
            self.logger.debug(f"Processed data: {processed_data}")
            if not processed_data:
                self.logger.debug("failed to process update data")
                return None

            # Check performance metrics periodically
            if self.metrics.should_log_metrics():
                self.metrics.log_all_metrics()

            # Check positions for risks
            if update_type not in [UpdateType.OnPosition, UpdateType.OnSpotTrade, UpdateType.OnDerivativeTrade]:
                self.metrics.check_all_positions()

            # Make trading decision
            self.logger.debug(f"Executing strategy {self.name}: {update_type}...")
            result = await self._execute_strategy(update_type, processed_data)
            if result:
                # Add strategy-wide defaults to orders
                for order in result.orders:
                    if not order.fee_recipient:
                        order.fee_recipient = self.fee_recipient
                    if not order.cid:
                        order.cid = self._generate_cid()
            return result

        except Exception as e:
            self.logger.error(f"Error executing strategy update: {e}")
            self.metrics.add_custom_alert(
                AlertType.SYSTEM,
                AlertSeverity.WARNING,
                f"Strategy execution error: {e}"
            )
            return None

    @abstractmethod
    async def _execute_strategy(
        self,
        update_type: UpdateType,
        processed_data: Dict
    ) -> StrategyResult | None:
        """
        Strategy-specific execution logic.

        Has access to:
        - Processed update data
        - All markets via self.markets
        - All accounts via self.accounts
        - Strategy configuration via self.config
        """
        pass

    def is_applicable(self, accounts: Dict[str, Account], markets: Dict[str, Market]) -> bool:
        """
        Check if strategy should be applied given current market conditions.
        Override this method to add custom activation logic.

        Args:
            accounts: Dictionary of account_address -> Account
            markets: Dictionary of market_id -> Market

        Returns:
            bool: True if strategy should be applied, False otherwise

        Example:
        ```python
        def is_applicable(self, accounts: Dict[str, Account], markets: Dict[str, Market]) -> bool:
            # Check if we have sufficient balance
            min_balance = Decimal("1000")
            for account_address in self.account_addresses:
                balance = self._get_total_balance(accounts[account_address])
                if balance < min_balance:
                    return False

            # Check market conditions
            for market_id in self.market_ids:
                market = markets[market_id]
                bid, ask = market.orderbook.tob()
                if not bid or not ask:
                    return False
                # Check spread
                spread = (ask - bid) / bid
                if spread > Decimal("0.01"):  # 1% max spread
                    return False

            return True
        ```
        """
        try:
            # Basic validation
            for market_id in self.market_ids:
                if market_id not in markets:
                    return False

            for account_address in self.account_addresses:
                if account_address not in accounts:
                    return False

            return True

        except Exception as e:
            self.logger.error(f"Error checking strategy applicability: {e}")
            return False

    def round_to_tick(self, value: Decimal, tick_size: Decimal) -> Decimal:
        """
        Round value to nearest tick size.
        Override this method if you need custom rounding logic.

        Args:
            value: Value to round
            tick_size: Tick size to round to

        Returns:
            Decimal: Rounded value

        Example:
        ```python
        def round_to_tick(self, value: Decimal, tick_size: Decimal) -> Decimal:
            # Round down for sells, up for buys
            if self._is_sell:
                return (value // tick_size) * tick_size
            return ((value + tick_size - Decimal("0.00000001")) // tick_size) * tick_size
        ```
        """
        return Decimal(value // tick_size * tick_size)

    def get_position(self, subaccount_id: str, market_id: str) -> Optional[Position]:
        """
        Retrieve a specific position by subaccount and market

        Args:
            subaccount_id: Subaccount identifier
            market_id: Market identifier

        Returns:
            Position data dictionary or None if position not found
        """
        return self.current_positions.get((subaccount_id, market_id))
