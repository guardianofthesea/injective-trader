from importlib import import_module
from typing import Dict, List, Optional, Type
from decimal import Decimal
import logging

from injective_trader.core.mediator import Mediator
from injective_trader.managers.account_manager import AccountManager
from injective_trader.managers.market_manager import MarketManager
from injective_trader.managers.strategy_manager import StrategyManager
from injective_trader.managers.risk_manager import RiskManager
from injective_trader.core.task_manager import TaskManager
from injective_trader.strategy.strategy import Strategy, StrategyResult  # Fixed import
from injective_trader.domain.message import Notification
from injective_trader.domain.account.order import Order
from injective_trader.core.state import IdleState
from injective_trader.utils.enums import Event, Side
from injective_trader.utils.logger import ThreadLogger
from injective_trader.utils.config import read_config

StrategyType = Type[Strategy]

class TraderClientError(Exception):
    """Custom exception for TraderClient errors"""
    pass


class TraderClient:
    """
    Main entry point for the injective-trader SDK.
    Encapsulates the complex framework setup into a simpler interface.
    """

    def __init__(self,
                config_path: Optional[str] = None,
                network: str = "mainnet",
                log_level: str = "INFO",
                debug: bool = False):
        """
        Initialize the trading client.

        Args:
            config_path: Path to configuration file (YAML)
            network: Network to connect to ("mainnet" or "testnet")
            log_path: Path for log file
            console_level: Console logging level
            file_level: File logging level
            debug: Enable debug mode (no real transactions)
        """
        self.config_path = config_path
        self.network = network
        self.debug = debug

        # Initialize logger
        try:
            self.thread_logger = ThreadLogger(
                name=f"injective-trader",
                signal_level=log_level,
            )
            self.logger = self.thread_logger.get_logger()
        except Exception as e:
            raise TraderClientError(f"Failed to initialize logger: {e}")

        # Initialize state
        self.config = None
        self.mediator = None
        self.task_manager = None
        self.initializer = None
        self.components = []
        self.optional_components = {}
        self.strategies = {}
        self._initialized = False
        self._running = False

        # Load configuration
        self._load_configuration()

    def add_log_handler(self, handler: logging.Handler):
        self.thread_logger.add_handler(handler)

    def remove_log_handler(self, handler: logging.Handler):
        self.thread_logger.remove_handler(handler)

    def _load_configuration(self):
        """Load configuration with proper error handling"""
        try:
            if self.config_path:
                self.config = read_config(self.config_path, self.logger)
                self.logger.info(f"Loaded configuration from {self.config_path}")
            else:
                from injective_trader.utils.config import create_default_config
                self.config = create_default_config(
                    logger=self.logger,
                    network=self.network,
                )
                self.logger.info("Using default configuration")
        except Exception as e:
            raise TraderClientError(f"Failed to load configuration: {e}")

    async def initialize(self):
        """
        Initialize the trading framework with the provided configuration.
        """
        if self._initialized:
            self.logger.warning("Client already initialized")
            return self

        try:
            self.logger.info("Initializing trading client...")

            # Initialize mediator
            self.mediator = Mediator(self.logger)

            # Initialize managers
            self._init_managers()

            # Initialize exchange-specific components
            await self._init_exchange_components()

            # Initialize strategies from config if any
            # if hasattr(self.config, "Strategies") and self.config.Strategies:
            #     await self._init_strategies_from_config()

            self._initialized = True
            self.logger.info("Trading client initialized successfully")
            return self

        except Exception as e:
            self.logger.error(f"Failed to initialize trading client: {e}")
            raise TraderClientError(f"Initialization failed: {e}")

    def _init_managers(self):
        """Initialize framework managers"""
        try:
            for manager_cls in (AccountManager, MarketManager, StrategyManager, RiskManager):
                self.logger.debug(f"Initializing {manager_cls.__name__}")
                self.mediator.add_manager(manager_cls(self.logger))

            self.task_manager = TaskManager(self.logger)
            self.logger.info("Core managers initialized successfully")
        except Exception as e:
            raise TraderClientError(f"Failed to initialize managers: {e}")

    async def _init_exchange_components(self):
        """Initialize exchange-specific components"""
        try:
            exchange = self.config.Exchange
            self.logger.info(f"Setting up {exchange} exchange components...")

            # Initialize initializer
            from injective_trader.utils.utils import get_initializer
            initializer_config = self.config.get_component_config("Initializer")
            self.initializer = get_initializer(exchange)(self.logger, initializer_config, self.mediator)

            # Initialize client, markets, and accounts
            composer_base = await self.initializer.initialize_client()
            await self.initializer.initialize_markets()
            await self.initializer.initialize_account()

            # Setup components
            await self._setup_required_components(exchange, composer_base)
            await self._setup_optional_components(exchange, composer_base)

        except Exception as e:
            self.logger.error(f"Failed to initialize exchange components: {e}")
            raise TraderClientError(f"Exchange component initialization failed: {e}")

    async def _setup_required_components(self, exchange: str, composer_base: dict):
        """Setup required components (ChainListener and MessageBroadcaster)"""
        try:
            required_components = [
                ("chain_listener", "ChainListener"),
                ("message_broadcaster", "MessageBroadcaster"),
            ]

            for component_type, class_suffix in required_components:
                module = import_module(f"injective_trader.agents.{component_type}.{exchange.lower()}_{component_type}")
                component_class = getattr(module, f"{exchange.capitalize()}{class_suffix}")

                if component_type == "message_broadcaster":
                    component = component_class(self.logger, composer_base, self.debug)
                else:
                    component = component_class(self.logger, composer_base)

                self.components.append(component)
                self.mediator.add_component(component)
                state_config = self._get_component_state_config(component)
                await component.set_state(IdleState(), **state_config["idle_state_args"])
                self.logger.info(f"{component.__class__.__name__} initialized")

        except Exception as e:
            raise TraderClientError(f"Failed to setup required components: {e}")

    async def _setup_optional_components(self, exchange: str, composer_base: dict):
        """Setup optional components that were added via add_component"""
        optional_components_path = {
            "RedisListener": "data_feed.redis.redis_listener",
            "ValkeyListener": "data_feed.valkey.valkey_listener",
            "HelixLiquidator": "liquidator.helix_liquidator",
            "HelixL3Streamer": "streamer.helix_l3_streamer",
        }

        try:
            for class_name, component_config in self.optional_components.items():
                if class_name not in optional_components_path:
                    self.logger.warning(f"Unknown optional component: {class_name}")
                    continue

                module_path = f"injective_trader.agents.{optional_components_path[class_name]}"
                module = import_module(module_path)

                component_class = getattr(module, class_name)

                init_args = [self.logger, composer_base]
                if "init_args" in component_config:
                    init_args.extend(component_config["init_args"])

                component = component_class(*init_args)
                self.components.append(component)
                self.mediator.add_component(component)

                state_config = self._get_component_state_config(component, component_config.get("config", {}))
                await component.set_state(IdleState(), **state_config["idle_state_args"])
                self.logger.info(f"Initialized optional component {class_name}")

        except Exception as e:
            self.logger.warning(f"Failed to setup optional component: {e}")

    def _get_component_state_config(self, component, additional_config: None|dict = None) -> dict:
        """Get component configuration for different states"""
        component_name = component.__class__.__name__
        for exchange_prefix in ["Helix", "Binance", "Ftx"]:
            if component_name.startswith(exchange_prefix):
                component_name = component_name[len(exchange_prefix):]
                break

        base_config = self.config.get_component_config(component_name)
        if additional_config:
            base_config = {**base_config, **additional_config}

        common_args = {"config": base_config}
        return {
            "idle_state_args": common_args,
            "running_state_args": common_args,
            "terminated_state_args": common_args
        }

    async def _init_strategies_from_config(self):
        """Initialize strategies from configuration using the initializer"""
        try:
            if not hasattr(self.config, "Strategies") or not self.config.Strategies:
                self.logger.info("No strategies configured")
                return

            # Use the initializer's strategy initialization method
            strategy_manager:StrategyManager|None = self.mediator.managers.get("StrategyManager")
            if not strategy_manager:
                raise TraderClientError("StrategyManager not found")

            await self.initializer.initialize_strategies(strategy_manager, self.config.Strategies)

            # Track initialized strategies in the client
            for strategy_name in self.config.Strategies.keys():
                if strategy_name in strategy_manager.strategies:
                    self.strategies[strategy_name] = strategy_manager.strategies[strategy_name]
                    self.logger.info(f"Tracked strategy: {strategy_name}")

        except Exception as e:
            self.logger.error(f"Error initializing strategies from config: {e}")
            raise TraderClientError(f"Strategy initialization failed: {e}")

    async def add_strategy(self, strategy_name: str, strategy_class: StrategyType, strategy_config: Optional[Dict] = None):
        """
        Add a trading strategy to the framework.

        Args:
            strategy_class: Strategy class to instantiate
            config: Configuration for the strategy
            strategy_name: Name of the strategy

        Returns:
            strategy: Initialized strategy object
        """
        if not self._initialized:
            await self.initialize()

        try:
            # Get strategy configuration
            if strategy_config is None:
                if hasattr(self.config, "Strategies") and strategy_name in self.config.Strategies:
                    strategy_config = self.config.Strategies[strategy_name]
                else:
                    strategy_config = {"Name": strategy_name, **strategy_config}

            # Use the initializer's method for consistency
            strategy_manager:StrategyManager|None = self.mediator.managers.get("StrategyManager")
            if not strategy_manager:
                raise TraderClientError("StrategyManager not found")

            # Use initializer's method (but only for this strategy)
            await self.initializer.initialize_strategy(strategy_manager, strategy_class, strategy_config)

            # Get the initialized strategy
            strategy = strategy_manager.strategies.get(strategy_config.get("Name", strategy_name))
            if not strategy:
                raise TraderClientError(f"Strategy {strategy_name} was not properly initialized")

            # Track strategy in client
            self.strategies[strategy.name] = strategy

            self.logger.info(f"Successfully added strategy: {strategy.name}")
            return strategy

        except Exception as e:
            self.logger.error(f"Failed to add strategy: {e}")
            raise TraderClientError(f"Strategy addition failed: {e}")

    async def place_order(self,
                         market_id: str,
                         subaccount_id: str,
                         side: str,
                         price: float,
                         quantity: float,
                         order_type: str = "limit",
                         trading_mode: str = "direct",
                         trading_account: Optional[str] = None,
                         fee_recipient: str = "",
                         margin: Optional[float] = None) -> Dict:
        """
        Place an order on the exchange.

        Args:
            market_id: Market ID to trade
            subaccount_id: Subaccount ID to use
            side: Order side ("buy" or "sell")
            price: Order price
            quantity: Order quantity
            order_type: Order type ("limit" or "market")
            trading_mode: Trading mode ("direct" or "authz")
            trading_account: Trading account address (for direct mode)
            fee_recipient: Fee recipient address
            margin: Margin amount (for derivative orders)

        Returns:
            dict: Order placement result
        """
        if not self._initialized:
            raise TraderClientError("Client not initialized. Call initialize() first.")

        try:
            # Validate inputs
            if market_id not in self.mediator.markets:
                raise TraderClientError(f"Market {market_id} not found")

            market = self.mediator.markets[market_id]

            # Convert side string to enum
            order_side = Side.BUY if side.lower() == "buy" else Side.SELL

            # Create order
            order = Order(
                market_id=market_id,
                market_type=market.market_type,
                subaccount_id=subaccount_id,
                order_side=order_side,
                price=Decimal(str(price)),
                quantity=Decimal(str(quantity)),
                order_type=order_type.upper(),
                fee_recipient=fee_recipient,
                margin=Decimal(str(margin)) if margin else None
            )

            # Create strategy result
            result = StrategyResult(orders=[order])

            # Determine trading info
            if trading_mode == "direct":
                if not trading_account:
                    # Use first available account that can sign
                    for addr, account in self.mediator.accounts.items():
                        if account.can_sign_transaction():
                            trading_account = addr
                            break
                    if not trading_account:
                        raise TraderClientError("No signing account available for direct trading")

                trading_info = {"trading_account": trading_account}
                account_data = {"trading_account": self.mediator.accounts[trading_account]}
            else:
                raise TraderClientError("Authz mode not implemented in place_order method")

            # Send broadcast notification
            notification = Notification(
                event=Event.BROADCAST,
                data={
                    "result": result,
                    "trading_mode": trading_mode,
                    "trading_info": trading_info,
                    "account_data": account_data
                }
            )

            await self.mediator.notify(notification)

            return {
                "success": True,
                "order_id": order.cid or "pending",
                "message": "Order submitted successfully"
            }

        except Exception as e:
            self.logger.error(f"Failed to place order: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": "Order placement failed"
            }

    async def cancel_order(self, market_id: str, subaccount_id: str, order_hash: str) -> Dict:
        """
        Cancel an existing order.

        Args:
            market_id: Market ID
            subaccount_id: Subaccount ID
            order_hash: Hash of order to cancel

        Returns:
            dict: Cancellation result
        """
        if not self._initialized:
            raise TraderClientError("Client not initialized. Call initialize() first.")

        try:
            # Validate market exists
            if market_id not in self.mediator.markets:
                raise TraderClientError(f"Market {market_id} not found")

            market = self.mediator.markets[market_id]

            # Create cancellation
            cancellation = {
                "market_id": market_id,
                "subaccount_id": subaccount_id,
                "order_hash": order_hash,
                "market_type": market.market_type
            }

            # Create strategy result
            result = StrategyResult(cancellations=[cancellation])

            # Find signing account
            trading_account = None
            for addr, account in self.mediator.accounts.items():
                if account.can_sign_transaction():
                    trading_account = addr
                    break

            if not trading_account:
                raise TraderClientError("No signing account available")

            # Send broadcast notification
            notification = Notification(
                event=Event.BROADCAST,
                data={
                    "result": result,
                    "trading_mode": "direct",
                    "trading_info": {"trading_account": trading_account},
                    "account_data": {"trading_account": self.mediator.accounts[trading_account]}
                }
            )

            await self.mediator.notify(notification)

            return {
                "success": True,
                "message": "Order cancellation submitted"
            }

        except Exception as e:
            self.logger.error(f"Failed to cancel order: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": "Order cancellation failed"
            }

    async def start(self):
        """Start the trading framework."""
        if not self._initialized:
            await self.initialize()

        if self._running:
            self.logger.warning("Trading client already running")
            return

        try:
            self.logger.info("Starting trading client...")

            # Start components
            for component in self.components:
                component_config = self.config.get_component_config(component.__class__.__name__)
                self.task_manager.add_task(component,
                    running_state_args=component_config,
                    terminated_state_args=component_config,
                    idle_state_args=component_config)

            # Start mediator queue processing
            self.task_manager.add_task_noncomponent(
                self.mediator.process_queues,
                "MediatorTask",
                runnable=self.mediator.process_queues
            )

            # Setup signal handler
            # loop = asyncio.get_running_loop()
            # self.task_manager.setup_signal_handler(loop)

            self._running = True
            self.logger.info("Trading client started successfully")

            # Run the task manager
            await self.task_manager.run()

        except Exception as e:
            self.logger.error(f"Error starting trading client: {e}")
            raise TraderClientError(f"Failed to start client: {e}")

    async def stop(self):
        """Stop the trading framework."""
        if not self._running:
            return

        try:
            self.logger.info("Stopping trading client...")
            await self.task_manager.shutdown("Client shutdown requested")
            await self.mediator.shutdown()

            self._running = False
            self.logger.info("Trading client stopped successfully")

        except Exception as e:
            self.logger.error(f"Error stopping trading client: {e}")
        finally:
            # Always shutdown logger
            if self.thread_logger:
                self.thread_logger.shutdown()

    async def get_markets(self, tickers: Optional[List[str]] = None) -> Dict:
        """
        Get market information.

        Args:
            tickers: Optional list of market tickers to filter

        Returns:
            dict: Market information keyed by market ID
        """
        if not self._initialized:
            await self.initialize()

        try:
            if not tickers:
                return {
                    market_id: {
                        "ticker": market.market_ticker,
                        "market_type": market.market_type.name,
                        "status": market.market_status,
                        "base_denom": getattr(market, 'base_denom', None),
                        "quote_denom": market.quote_denom,
                        "min_price_tick": float(market.min_price_tick) if hasattr(market, 'min_price_tick') else None,
                        "min_quantity_tick": float(market.min_quantity_tick) if hasattr(market, 'min_quantity_tick') else None,
                    }
                    for market_id, market in self.mediator.markets.items()
                }

            # Filter by tickers
            filtered_markets = {}
            for ticker in tickers:
                market_id = self.mediator.ticker_to_id.get(ticker)
                if market_id and market_id in self.mediator.markets:
                    market = self.mediator.markets[market_id]
                    filtered_markets[market_id] = {
                        "ticker": market.market_ticker,
                        "market_type": market.market_type.name,
                        "status": market.market_status,
                        "base_denom": getattr(market, 'base_denom', None),
                        "quote_denom": market.quote_denom,
                        "min_price_tick": float(market.min_price_tick) if hasattr(market, 'min_price_tick') else None,
                        "min_quantity_tick": float(market.min_quantity_tick) if hasattr(market, 'min_quantity_tick') else None,
                    }

            return filtered_markets

        except Exception as e:
            self.logger.error(f"Error getting markets: {e}")
            raise TraderClientError(f"Failed to get markets: {e}")

    async def get_accounts(self) -> Dict:
        """
        Get account information.

        Returns:
            dict: Account information keyed by account address
        """
        if not self._initialized:
            await self.initialize()

        try:
            return {
                addr: {
                    "address": account.account_address,
                    "can_sign": account.can_sign_transaction(),
                    "sequence": account.sequence,
                    "bank_balances": {
                        denom: float(balance.amount)
                        for denom, balance in account.bank_balances.items()
                    },
                    "subaccounts": list(account.subaccounts.keys())
                }
                for addr, account in self.mediator.accounts.items()
            }
        except Exception as e:
            self.logger.error(f"Error getting accounts: {e}")
            raise TraderClientError(f"Failed to get accounts: {e}")

    async def get_positions(self, account_address: Optional[str] = None) -> Dict:
        """
        Get position information.

        Args:
            account_address: Optional account address to filter

        Returns:
            dict: Position information
        """
        if not self._initialized:
            await self.initialize()

        try:
            positions = {}
            accounts_to_check = [account_address] if account_address else list(self.mediator.accounts.keys())

            for addr in accounts_to_check:
                if addr not in self.mediator.accounts:
                    continue

                account = self.mediator.accounts[addr]
                positions[addr] = {}

                for subaccount_id, subaccount in account.subaccounts.items():
                    for market_id, position in subaccount.positions.items():
                        if position.quantity != 0:  # Only include non-zero positions
                            market_ticker = self.mediator.id_to_ticker.get(market_id, market_id)
                            positions[addr][f"{subaccount_id}_{market_ticker}"] = {
                                "market_id": market_id,
                                "market_ticker": market_ticker,
                                "subaccount_id": subaccount_id,
                                "quantity": float(position.quantity),
                                "entry_price": float(position.entry_price),
                                "margin": float(position.margin),
                                "is_long": position.is_long,
                                "unrealized_pnl": float(position.unrealized_pnl)
                            }

            return positions

        except Exception as e:
            self.logger.error(f"Error getting positions: {e}")
            raise TraderClientError(f"Failed to get positions: {e}")

    async def get_orderbook(self, market_id: str) -> Dict:
        """
        Get orderbook for a specific market.

        Args:
            market_id: Market ID

        Returns:
            dict: Orderbook data
        """
        if not self._initialized:
            await self.initialize()

        try:
            if market_id not in self.mediator.markets:
                raise TraderClientError(f"Market {market_id} not found")

            market = self.mediator.markets[market_id]
            orderbook = market.orderbook

            return {
                "market_id": market_id,
                "ticker": market.market_ticker,
                "sequence": orderbook.sequence,
                "is_healthy": orderbook.is_healthy,
                "bids": [{"price": float(level.price), "quantity": float(level.quantity)}
                        for level in orderbook.bids[:10]],  # Top 10 levels
                "asks": [{"price": float(level.price), "quantity": float(level.quantity)}
                        for level in orderbook.asks[:10]]   # Top 10 levels
            }

        except Exception as e:
            self.logger.error(f"Error getting orderbook: {e}")
            raise TraderClientError(f"Failed to get orderbook: {e}")

    def add_component(self, component_name: str, config: None|Dict = None, init_args: None|List = None):
        """
        Add an optional component to the client.

        Args:
            component_name: Name of the component class
            config: Component configuration
            init_args: Additional initialization arguments
        """
        if config is None:
            if component_name in self.config.Components:
                config = self.config.Components[component_name]
            else:
                config = {}
        self.optional_components[component_name] = {
            "config": config,
            "init_args": init_args or []
        }
        self.logger.info(f"Added optional component: {component_name}")

    @property
    def is_initialized(self) -> bool:
        """Check if client is initialized"""
        return self._initialized

    @property
    def is_running(self) -> bool:
        """Check if client is running"""
        return self._running

    async def __aenter__(self):
        """Async context manager entry"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()
