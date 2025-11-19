#######################
## Mediator Patterns ##
#######################
from __future__ import annotations
from typing import Dict, List, TYPE_CHECKING
import asyncio
import time
import uuid
from decimal import Decimal

from injective_trader.utils.enums import Event, UpdateType
from pyinjective.core.market import SpotMarket, DerivativeMarket, BinaryOptionMarket
if TYPE_CHECKING:
    from injective_trader.domain.message import Notification
    from injective_trader.domain.account.account import Account
    from injective_trader.domain.market.market import Market
    from injective_trader.core.component import Manager, Component
    from injective_trader.strategy.strategy import Strategy


class Mediator:
    """
    The Mediator defines the interface of interest to clients. It also maintains
    a reference to an instance of a State subclass, which represents the current
    state of the Mediator.
    """

    def __init__(self, logger):
        """
        All the components are restored in a dictionary
        """
        self.name = "Mediator"
        self.logger = logger

        # Centralized data storage
        self.block_height: int = 0
        self.markets: Dict[str, Market] = {}
        self.accounts: Dict[str, Account] = {}

        # Reference dictionaries
        self.subaccount_to_account: Dict[str, str] = {}
        self.ticker_to_id: Dict[str, str] = {}  # market ticker -> market_id
        self.id_to_ticker: Dict[str, str] = {}  # market_id -> market ticker
        self.denom_decimals: Dict[str, Decimal] = {} # denom of token -> decimals of token
        self.denom_to_symbol: Dict[str, str] = {} # denom of token -> symbol of token
        self.symbol_to_denom: Dict[str, str] = {} # symbol of token -> denom of token
        self.oracle_symbol_to_market: Dict[str, List] = {} # symbol of token -> a list of market ids that contain this token

        # Restore all indices
        self.market_ids: List = []
        self.account_addresses: List = []
        self.subaccount_ids: List = []
        self.spot_market_ids: List = []
        self.perp_market_ids: List = []
        self.binary_market_ids: List = []
        self.oracle_symbols: List = []

        # Component Management
        self.components: Dict[str, Component] = {}
        self.managers: Dict[str, Manager] = {}
        self.data_update_queue = asyncio.PriorityQueue()
        self.internal_comm_queue = asyncio.PriorityQueue()
        self.broadcaster_queue = asyncio.PriorityQueue()

        self._counter = 0
        
        # Message timeout configuration (in seconds)
        self.message_timeouts = {
            Event.ORDERBOOK_UPDATE: 2.0,
            Event.ORACLE_UPDATE: 2.0,
            Event.EXTERNAL_INFO: 5.0,
            Event.BALANCE_UPDATE: 30.0,
            Event.POSITION_UPDATE: 30.0,
            Event.DATA_REQUEST: 10.0,
            Event.STRATEGY_UPDATE: 15.0,
            Event.RISK_UPDATE: 15.0,
            Event.CONFIG_UPDATE: 60.0,
        }
        self.default_timeout = 30.0  # Default timeout for unspecified events

    def _get_next_counter(self):
        self._counter += 1
        return self._counter

    def add_manager(self, manager: Manager):
        self.managers[manager.name] = manager
        manager.set_mediator(self)

    def add_component(self, component: Component):
        self.components[component.name] = component
        component.set_mediator(self)

    def add_market(self, market: Market):
        self.markets[market.market_id] = market
        self.market_ids.append(market.market_id)

        if isinstance(market.market, SpotMarket):
            self.spot_market_ids.append(market.market_id)
        elif isinstance(market.market, DerivativeMarket):
            self.perp_market_ids.append(market.market_id)
            self.oracle_symbols.append(market.market.oracle_base)
            self.oracle_symbols.append(market.market.oracle_quote)
        elif isinstance(market.market, BinaryOptionMarket):
            self.binary_market_ids.append(market.market_id)

    def add_account(self, account: Account):
        self.accounts[account.account_address] = account
        self.account_addresses.append(account.account_address)

        for subaccount_id, _ in account.subaccounts.items():
            self.subaccount_ids.append(subaccount_id)
            self.subaccount_to_account[subaccount_id] = account.account_address

    def add_strategy(self, strategy: Strategy):
        if "StrategyManager" not in self.managers:
            raise ValueError("StrategyManager is not initialized")
        self.managers["StrategyManager"].add_strategy(strategy)

    async def notify(self, notification: Notification):
        # Add qid and timestamp for tracking if not already present
        if not hasattr(notification, 'data') or notification.data is None:
            notification.data = {}
        if 'qid' not in notification.data:
            notification.data['qid'] = str(uuid.uuid4())
        if 'created_at' not in notification.data:
            notification.data['created_at'] = time.monotonic()
        
        qid = notification.data['qid']
        priority = self.calculate_priority(notification)
        count = self._get_next_counter()
        queue_item = (priority, count, notification)
        
        self.logger.debug(f"Queue item: event ({notification.event}), priority ({priority}), count ({count}), data ({notification.data})")

        if notification.event == Event.BROADCAST:
            self.logger.debug(f"Putting queue item {notification.event} to broadcaster_queue, qid ({qid})")
            await self.broadcaster_queue.put(queue_item)
        elif self._is_data_update_event(notification.event):
            self.logger.debug(f"Putting queue item {notification.event} to data_update_queue, qid ({qid})")
            await self.data_update_queue.put(queue_item)
        else:
            self.logger.debug(f"Putting queue item {notification.event} to internal_comm_queue, qid ({qid})")
            await self.internal_comm_queue.put(queue_item)

    def _is_data_update_event(self, event: Event) -> bool:
        """Determine if event is a data update (market/account data) vs internal communication."""
        data_update_events = {
            Event.ORDERBOOK_UPDATE, Event.ORACLE_UPDATE, Event.EXTERNAL_INFO,
            Event.BALANCE_UPDATE, Event.DEPOSIT_UPDATE, Event.POSITION_UPDATE, 
            Event.SPOT_ORDER_UPDATE, Event.DERIVATIVE_ORDER_UPDATE,
            Event.SPOT_TRADE_UPDATE, Event.DERIVATIVE_TRADE_UPDATE
        }
        return event in data_update_events

    def _is_message_expired(self, notification: Notification) -> bool:
        """Check if a message has exceeded its timeout."""
        if not notification.data or 'created_at' not in notification.data:
            return False
        
        created_at = notification.data['created_at']
        timeout = self.message_timeouts.get(notification.event, self.default_timeout)
        return (time.monotonic() - created_at) > timeout

    ### TODO: implement priority algorithms
    def calculate_priority(self, notification: Notification) -> int:
        if notification.event in [Event.ORDERBOOK_UPDATE, Event.ORACLE_UPDATE, Event.EXTERNAL_INFO]:
            return 1
        else:
            return 2

    async def _queue_consumer(self, queue: asyncio.PriorityQueue, queue_name: str):
        """A dedicated, blocking consumer for a single priority queue with diagnostics."""
        dropped_count = 0
        while True:
            try:
                # -- Diagnostic Step 1: Log queue size if it's growing --
                qsize = queue.qsize()
                if qsize > 100:
                    self.logger.warning(f"High load: {queue_name} size is {qsize}")

                # This will block efficiently until an item is available
                priority, _, notification = await queue.get()
                qid = notification.data.get('qid', 'unknown') if notification.data else 'unknown'
                
                # Check if message is expired - if so, drop it and continue
                if self._is_message_expired(notification):
                    dropped_count += 1
                    age = time.monotonic() - notification.data.get('created_at', time.monotonic())
                    self.logger.warning(f"Dropping expired message from {queue_name}: {notification.event}, age {age:.2f}s, qid ({qid})")
                    
                    # Log dropped count every 10 drops to avoid log spam
                    if dropped_count % 10 == 0:
                        self.logger.warning(f"Dropped {dropped_count} expired messages from {queue_name}")
                    continue

                self.logger.debug(f"Dequeued item from {queue_name} (qsize: {qsize}) with priority {priority}: {notification.event}, qid ({qid})")

                # -- Diagnostic Step 2: Time the processing of the notification --
                start_time = time.monotonic()
                await self.process_notification(notification)
                duration = time.monotonic() - start_time
                if duration > 0.5: # Log if processing takes more than 500ms
                    self.logger.warning(f"Slow processing: Event {notification.event} took {duration:.2f} seconds, qid ({qid})")

            except asyncio.CancelledError:
                self.logger.info(f"Queue consumer for {queue_name} cancelled.")
                break  # Exit the loop cleanly on cancellation
            except Exception as e:
                self.logger.error(f"Error in {queue_name} consumer: {e}", exc_info=True)
                # Optionally, add a small delay to prevent rapid-fire error loops
                await asyncio.sleep(1)

    async def process_queues(self):
        """Processes messages from all queues concurrently using dedicated consumers."""
        self.logger.info("Starting concurrent queue processors.")
        consumer_tasks = []
        try:
            # Create and run a dedicated consumer task for each queue
            broadcaster_consumer = asyncio.create_task(self._queue_consumer(self.broadcaster_queue, "BroadcasterQueue"))
            data_update_consumer = asyncio.create_task(self._queue_consumer(self.data_update_queue, "DataUpdateQueue"))
            internal_comm_consumer = asyncio.create_task(self._queue_consumer(self.internal_comm_queue, "InternalCommQueue"))
            consumer_tasks = [broadcaster_consumer, data_update_consumer, internal_comm_consumer]

            # This will run until one of the consumer tasks is cancelled or fails
            await asyncio.gather(*consumer_tasks)

        except asyncio.CancelledError:
            self.logger.info("Mediator process_queues task cancelled. Stopping consumers.")
            # The CancelledError is automatically propagated to the consumer tasks awaited in gather
            raise
        finally:
            self.logger.info("Mediator process_queues finished.")


    async def process_notification(self, notification: Notification):
        self.logger.debug(f"Processing notification with event {notification.event}")
        event = notification.event
        data = notification.data

        # Message Broadcast
        if event == Event.BROADCAST:
            broadcast_data = {
                "result": data["result"],
                "trading_mode": data["trading_mode"],
                "trading_info": data["trading_info"],
                "account_data": {}
            }

            # Add account information
            if data["trading_mode"] == "direct":
                trading_account = data["trading_info"]["trading_account"]
                if trading_account not in self.accounts:
                    raise ValueError(f"Trading account {trading_account} not found")
                broadcast_data["account_data"]["trading_account"] = self.accounts[trading_account]
            else: # authz mode
                granter = data["trading_info"]["granter"]
                grantees = data["trading_info"]["grantees"]

                if granter not in self.accounts:
                    raise ValueError(f"Granter account {granter} not found")

                grantee_accounts = {}
                for grantee in grantees:
                    if grantee not in self.accounts:
                        self.logger.warning(f"Grantee account {grantee} not found, skipping")
                        continue
                    grantee_accounts[grantee] = self.accounts[grantee]

                broadcast_data["account_data"]["granter_account"] = self.accounts[granter]
                broadcast_data["account_data"]["grantee_accounts"] = grantee_accounts

            await self.components["MessageBroadcaster"].receive(event, broadcast_data)

        # Configuration Detector
        elif event == Event.CONFIG_UPDATE:
            await self.components["ConfigWatcher"].receive(event, data)

        # Update Market Data
        elif event == Event.ORDERBOOK_UPDATE:
            data["market"] = self.markets[data["market_id"]]
            await self.managers["MarketManager"].receive(event, data)

        elif event == Event.ORACLE_UPDATE:
            symbol = data["symbol"]
            if symbol not in self.oracle_symbol_to_market:
                self.logger.debug(f"The oracle symbol {symbol} cannot be found in tokens of configured markets. skipping...")
                return
            market_ids = self.oracle_symbol_to_market[data["symbol"]]
            for market_id in market_ids:
                if market_id in self.markets:
                    data["market_id"] = market_id
                    data["market"] = self.markets[market_id]
                    await self.managers["MarketManager"].receive(event, data)

        # Initialize Account Data
        elif event == Event.ACCOUNT_INIT:
            account: Account = data["account"]
            self.accounts[account.account_address] = account
            self.logger.info(f"Initialized account {account.account_address}")

        # Update Account Data
        elif event in [Event.BALANCE_UPDATE, Event.DEPOSIT_UPDATE, Event.POSITION_UPDATE, Event.SPOT_ORDER_UPDATE, Event.DERIVATIVE_ORDER_UPDATE, Event.SPOT_TRADE_UPDATE, Event.DERIVATIVE_TRADE_UPDATE]:
            if "account_address" in data:
                data["account"] = self.accounts[data["account_address"]]
            elif "address" in data:
                data["account"] = self.accounts[data["address"]]
            elif "subaccount_id" in data:
                data["account"] = self.accounts[self.subaccount_to_account[data["subaccount_id"]]]
            await self.managers["AccountManager"].receive(event, data)

        # Strategy Update
        elif event == Event.STRATEGY_UPDATE:
            self.logger.debug(f"StrategyManager receives {event}, {data}")
            await self.managers["StrategyManager"].receive(event, data)

        # Data request from strategy and strategy execution
        elif event == Event.DATA_REQUEST:
            for market_id in data["market_ids"]:
                if market_id not in self.markets:
                    self.logger.warning(
                        f"Market {market_id} not found - this market needs to be registered in initializer config "
                        f"before it can be used in strategies"
                    )

            for account_address in data["account_addresses"]:
                if account_address not in self.accounts:
                    self.logger.warning(
                        f"Account {account_address} not found - this market needs to be registered in initializer config "
                        f"before it can be used in strategies"
                    )

            strategy_execution_data = {
                "strategy_name": data["strategy_name"],
                "markets": {
                    mid: self.markets[mid]
                    for mid in data["market_ids"]
                },
                "accounts": {
                    aid: self.accounts[aid]
                    for aid in data["account_addresses"]
                },
                "update_type": data["update_type"],
                "update_data": data["update_data"],
            }
            new_event = Event.STRATEGY_EXECUTION
            await self.managers["StrategyManager"].receive(new_event, strategy_execution_data)

        # Risk update
        elif event == Event.RISK_UPDATE:
            await self.managers["RiskManager"].receive(event, data)

        elif event == Event.LIQUIDATION_INFO:
            data["update_type"] = UpdateType.OnLiquidatablePosition
            await self.managers["StrategyManager"].receive(Event.STRATEGY_UPDATE, data)

        elif event == Event.EXTERNAL_INFO:
            data["update_type"] = UpdateType.OnExternalInfo
            await self.managers["StrategyManager"].receive(Event.STRATEGY_UPDATE, data)

        else:
            raise Exception(f"Event {event} has not been implemented.")

    def _build_strategy_payload(
        self,
        *,
        raw: dict,
        update_type: UpdateType,
        default_market_ids: list[str] | None = None,
        default_account_addrs: list[str] | None = None,
    ) -> Dict:
        """
        Convert an arbitrary info / risk / external signal into the canonical
        payload that StrategyManager understands.

        Parameters
        ----------
        raw : dict
            The original event‐specific data (passed through untouched in
            `update_data` so strategies can inspect full context if needed).
        update_type : UpdateType
            Semantic meaning of the event (e.g. OnExternalInfo,
            OnLiquidatablePosition, …).
        default_market_ids : list[str] | None
            Market(s) implicated by the event if the producer didn’t specify
            explicit `market_ids` in `raw`.
        default_account_addrs : list[str] | None
            Account(s) implicated by the event if the producer didn’t specify
            explicit `account_addresses` in `raw`.
        """
        # ─── Routing ───
        strategy_name = raw.get("strategy_name")

        # ─── Markets ───
        market_ids = raw.get("market_ids") or default_market_ids or []
        markets = {mid: self.markets[mid] for mid in market_ids if mid in self.markets}

        # ─── Accounts ───
        account_addrs = raw.get("account_addresses") or default_account_addrs or []
        accounts = {
            addr: self.accounts[addr] for addr in account_addrs if addr in self.accounts
        }

        return {
            "strategy_name": strategy_name,
            "markets":        markets,
            "accounts":       accounts,
            "update_type":    update_type,
            "update_data":    raw,
        }

    async def shutdown(self):
        self.logger.info("Shutting down Mediator")
        # Clear queues or perform any necessary cleanup
        while not self.data_update_queue.empty():
            self.data_update_queue.get_nowait()
        while not self.internal_comm_queue.empty():
            self.internal_comm_queue.get_nowait()
        while not self.broadcaster_queue.empty():
            self.broadcaster_queue.get_nowait()
        self.logger.info("Mediator shutdown complete")
