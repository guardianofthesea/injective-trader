from typing import Dict, Any, List, Set, Awaitable
from decimal import Decimal
import grpc
from grpc import RpcError
import asyncio
from datetime import datetime, timedelta
import traceback
import uuid
import time

from pyinjective.core.market import DerivativeMarket

from injective_trader.core.component import Component
#from injective_trader.domain.account.order import Order
from injective_trader.domain.market import orderbook
from injective_trader.domain.message import Notification
from injective_trader.domain.market.market import Market
from injective_trader.utils.enums import Event, Exchange, Side, MarketType
from injective_trader.utils.helpers import (
    order_hash_from_chain_format,
    #update_indexer_order_book_to_human_readable,
    update_order_book_to_human_readable,
)
from injective_trader.utils.retry import get_retry_handler
from injective_trader.utils.throttle_condtion import ThrottledCondition
from injective_trader.utils.oracle_type import ORACLE_DECIMALS, PYTH_FEED_ID_TO_SYMBOL, STORK_ORACLE_SYMBOL, PROVIDER_ORACLE_SYMBOL


class HelixChainListener(Component):
    RECONNECTION_DELAY = 5
    LARGE_GAP_THRESHOLD = 2
    MAX_QUEUE_SIZE = 100

    def __init__(self, logger, composer_base):
        super().__init__(logger, composer_base)
        self.name = "ChainListener"
        self._stream_active = False
        self._stream_task: asyncio.Task | None = None
        self._snapshot_task: asyncio.Task | None = None
        self._sequence_tracker: Dict[str, int] = {} # market_id -> last sequence

        # Add buffer for updates that arrive during initialization
        self._update_buffer: Dict[str, asyncio.PriorityQueue[tuple[int,Notification]]] = {}  # market_id -> list of updates
        self._condition: ThrottledCondition = ThrottledCondition(5, self.logger)

    ##############################
    ### Core Interface Methods ###
    ##############################

    async def initialize(self, **kwargs):
        """
        Initialize chain listener with configuration
        """
        self._stream_active = True

        # Get chain-specific configuration and setup retry handler
        self.config = kwargs.get("config", {})
        retry_config = self.config.get("RetryConfig", {})
        self.retry_handler = get_retry_handler(
            self.logger,
            retry_config,
            "ChainListener"
        )

        self.reconnection_delay = self.config.get("ReconnectionDelay", self.RECONNECTION_DELAY)
        self.large_gap_threshold = self.config.get("LargeGapThreshold", self.LARGE_GAP_THRESHOLD)
        self.max_queue_size = self.config.get("MaxQueueSize", self.MAX_QUEUE_SIZE)

        # Get token decimals and denom to symbol from mediator (if available)
        self.denom_decimals = self.mediator.denom_decimals if hasattr(self.mediator, 'denom_decimals') else {}
        self.denom_to_symbol = self.mediator.denom_to_symbol if hasattr(self.mediator, 'denom_to_symbol') else {}
        self.oracle_symbols = self.mediator.oracle_symbols

        self.logger.info("Helix ChainListener initialized successfully")

    async def run(self, **kwargs):
        """
        Starts listening to the chain stream and processes incoming events.
        Sets up filters for specific types of events such as trades, orders, and order books.
        """
        self.logger.info("--------------------------------Starting chain stream listener--------------------------------")
        for market_id in self.mediator.market_ids:
            self.logger.info(f"Initializing sequence tracker")
            orderbook = self.mediator.markets[market_id].orderbook
            orderbook.is_healthy = False # Set this to false because indexer is not very reliable.
            self._sequence_tracker[market_id] = orderbook.sequence
            self._update_buffer[market_id] = asyncio.PriorityQueue(maxsize=self.max_queue_size)
            self.logger.info(f"Sequence tracker {self.mediator.id_to_ticker[market_id]} current sequence: {self._sequence_tracker[market_id]}")
        self._stream_active = True

        async def stream_operation():
            self.logger.info("Started stream operation")
            filters = await self._process_filters()
            self._stream_task = asyncio.create_task(
                self.client.listen_chain_stream_updates(
                    callback=self.chain_stream_event_processor,
                    on_end_callback=self.stream_closed_processor,
                    on_status_callback=self.stream_error_processor,
                    **filters
                ),
                name="chain stream"
            )
            # Wait for the task to complete or be cancelled
            await self._stream_task

        async def snapshot_operation():
            self.logger.info("Started snapshot operation")
            self._snapshot_task = asyncio.create_task(self.request_snapshot(), name="snapshot task")
            # Wait for the task to complete or be cancelled
            await self._snapshot_task

        async def _wrap(name: str, coro: Awaitable[None]):
            try:
                await coro
            except Exception as e:
                tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                self.logger.error(f"{name} failed: {e}\nTraceback:\n{tb}")
                raise

        while self._stream_active:
            try:
                async with asyncio.TaskGroup() as task_group:
                    task_group.create_task(_wrap(
                        "stream_operation",
                        self.retry_handler.execute_with_retry(stream_operation),
                    ))
                    task_group.create_task(_wrap(
                        "snapshot_operation",
                        self.retry_handler.execute_with_retry(snapshot_operation),
                    ))

            except* Exception as eg:
                if self._stream_active:
                    await asyncio.sleep(self.reconnection_delay)

    async def request_snapshot(self):
        self.logger.info("Started request snapshots")
        while True:
            self.logger.info("Waiting for next notification")
            # I"m getting a market_id here, so I know which orderbook has a gap.
            market_id = await self._condition.wait()
            if market_id is None:
                raise Exception("Missing market_id")
            try:
                self.logger.info(f"Requesting the lastest orderbook snapshot: {self.mediator.id_to_ticker[market_id]}")
                await self._request_orderbook_snapshot(market_id)
                self.logger.info(f"Received the lastest orderbook for market: {self.mediator.id_to_ticker[market_id]}")
            except Exception as e:
                self.logger.error(f"Error in requesting orderbook snapshot: {e}.")
                raise e

            self.fast_update_orderbook(market_id)

    def fast_update_orderbook(self, market_id:str):
        while not self._update_buffer[market_id].empty():
            try:
                # Get item without waiting
                (_,buffered_notification) = self._update_buffer[market_id].get_nowait()
                sequence = buffered_notification.data['sequence']
                if sequence == self._sequence_tracker[market_id]+1:
                    data = buffered_notification.data
                    updates = data['updates']
                    sequence = data['sequence']
                    market_ticker = self.mediator.id_to_ticker[market_id]
                    orderbook = self.mediator.markets[market_id].orderbook
                    orderbook.batch_update_orderbook(updates=updates, sequence=sequence, market_ticker=market_ticker, force_update=False)
                    self._sequence_tracker[market_id] = orderbook.sequence
                    self.logger.info(f"Updated {self.mediator.id_to_ticker[market_id]} orderbook snapshot sequence tracker {self._sequence_tracker[market_id]}")
                else:
                    self.logger.info(f"Ignored buffered notification: {sequence}/{self._sequence_tracker[market_id]}")
            except Exception as e:
                self.logger.Error(f"Error in processing orderbook {e}.")
                raise e

    async def terminate(self, msg: str, **kwargs):
        """
        Clean shutdown of the chain listener
        """
        self.logger.info(f"Terminating chain listener: {msg}")
        self._stream_active = False

        if self._stream_task and not self._stream_task.done():
            self.logger.info(f"Explicitly cancelling stream task in terminate method")
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                self.logger.info("Stream task cancelled successfully in terminate")
            except Exception as e:
                self.logger.error(f"Error while waiting for cancelled stream task: {e}")

        if self._snapshot_task and not self._snapshot_task.done():
            self.logger.info(f"Explicitly cancelling snapshot task in terminate method")
            self._snapshot_task.cancel()
            try:
                await self._snapshot_task
            except asyncio.CancelledError:
                self.logger.info("Snapshot task cancelled successfully in terminate")
            except Exception as e:
                self.logger.error(f"Error while waiting for cancelled stream task: {e}")

        await super().terminate(msg, **kwargs)

    async def receive(self, event, data):
        pass

    ########################
    ### Stream Lifecycle ###
    ########################

    async def chain_stream_event_processor(self, event: Dict):
        async def process_operation():
            # if (
            #     event["oraclePrices"]
            #     or event["positions"]
            #     or event["derivativeTrades"]
            #     or event["derivativeOrders"]
            #     or event["derivativeOrderbookUpdates"]
            #     or event["spotOrders"]
            #     or event["spotTrades"]
            #     or event["spotOrderbookUpdates"]
            #     or event["subaccountDeposits"]
            #     or event["bankBalances"]
            # ):
            #     self.logger.info(f"Processing chain stream event")
            # else:
            #     return
            block_height = int(event.get("blockHeight", 0))
            if not block_height:
                raise ValueError("Block height not found in event data.")
            self.mediator.block_height=block_height

            # Parse block time if available
            block_time = None
            block_time_str = event.get("blockTime", "")
            if block_time_str:
                try:
                    # Convert miliseconds timestamp to datetime
                    block_time = datetime.fromtimestamp(int(block_time_str) / 1000)
                except (ValueError, TypeError) as e:
                    self.logger.warning(
                        f"Failed to parse block time '{block_time_str}': {e}"
                    )
                    block_time = datetime.now()
            else:
                block_time = datetime.now()

            # Process events
            for update in event.get("spotOrderbookUpdates", []):
                await self._process_orderbook_update(update, block_time_str)

            for update in event.get("derivativeOrderbookUpdates", []):
                await self._process_orderbook_update(update, block_time_str)

            ### TODO: ORACLE PRICES
            for oracle_price in event.get("oraclePrices", []):
                await self._process_oracle_price(oracle_price, block_height, block_time)

            for bank_balance in event.get("bankBalances", []):
                await self._process_bank_balances(bank_balance)

            for subaccount_deposit in event.get("subaccountDeposits", []):
                await self._process_subaccount_deposits(subaccount_deposit)

            for position in event.get("positions", []):
                await self._process_positions(position)

            for spot_order in event.get("spotOrders", []):
                await self._process_spot_orders(spot_order)

            for derivative_order in event.get("derivativeOrders", []):
                await self._process_derivative_orders(derivative_order)

            for spot_trade in event.get("spotTrades", []):
                await self._process_spot_trades(spot_trade)

            for derivative_trade in event.get("derivativeTrades", []):
                await self._process_derivative_trades(derivative_trade)

        try:
            await self.retry_handler.execute_with_retry(
                operation=process_operation,
                context={"event_type": list(event.keys())[0]}
            )
        except Exception as e:
            self.logger.error(f"Error processing chain stream event after retries: {e}")


    def stream_error_processor(self, exception: RpcError):
        """
        Handles errors that occur while listening to the chain stream.

        :param exception: The exception raised during the chain stream.
        """
        error_msg = f"Stream error: {exception}"

        if isinstance(exception, grpc.aio._call.AioRpcError):
            status_code = exception.code()
            details = exception.details()
            debug_error_string = exception.debug_error_string()
            error_msg = f"gRPC Stream error (Status: {status_code}): {exception}\nDetails: {details}\nDebug: {debug_error_string}"

        self.logger.error(error_msg)
        raise exception

    def stream_closed_processor(self):
        """
        Handles the closure of the chain stream. This might occur if the connection is dropped
        or if the chain stream service is unavailable.
        """
        self.logger.info("Chain stream has been closed")
        raise Exception("Chain stream has been closed")
        # TODO: Implement reconnection logic here if desired


    ####################
    ### Stream Setup ###
    ####################

    async def _process_filters(self) -> Dict:
        """
        Create stream filters based on configuration
        """
        market_ids = self.mediator.market_ids
        spot_market_ids = self.mediator.spot_market_ids
        perp_market_ids = self.mediator.perp_market_ids
        binary_market_ids = self.mediator.binary_market_ids
        account_addresses = self.mediator.account_addresses
        subaccount_ids = self.mediator.subaccount_ids
        oracle_symbols = self.mediator.oracle_symbols

        # Initialize filters
        filters = {}

        if account_addresses:
            filters["bank_balances_filter"] = self.composer.chain_stream_bank_balances_filter(accounts=account_addresses)
        if subaccount_ids:
            filters["subaccount_deposits_filter"] = self.composer.chain_stream_subaccount_deposits_filter(subaccount_ids=subaccount_ids)

        if spot_market_ids:
            filters.update({
                "spot_trades_filter": self.composer.chain_stream_trades_filter(
                    subaccount_ids=subaccount_ids,
                    market_ids=spot_market_ids,
                ),
                "spot_orders_filter": self.composer.chain_stream_orders_filter(
                    subaccount_ids=subaccount_ids,
                    market_ids=spot_market_ids,
                ),
                "spot_orderbooks_filter": self.composer.chain_stream_orderbooks_filter(
                    market_ids=spot_market_ids
                )
            })

        if perp_market_ids:
            filters.update({
                "derivative_trades_filter": self.composer.chain_stream_trades_filter(
                    subaccount_ids=subaccount_ids,
                    market_ids=perp_market_ids,
                ),
                "derivative_orders_filter": self.composer.chain_stream_orders_filter(
                    subaccount_ids=subaccount_ids,
                    market_ids=perp_market_ids,
                ),
                "derivative_orderbooks_filter": self.composer.chain_stream_orderbooks_filter(
                    market_ids=perp_market_ids,
                ),
                "positions_filter": self.composer.chain_stream_positions_filter(
                    subaccount_ids=subaccount_ids,
                    market_ids=perp_market_ids,
                )
            })

            if oracle_symbols:
                ### FIXME: How can I the symbols we should listen to? What forms are they in?
                # filters["oracle_price_filter"] = self.composer.chain_stream_oracle_price_filter(
                #     symbols=oracle_symbols,
                # )
                filters["oracle_price_filter"] = self.composer.chain_stream_oracle_price_filter(
                    symbols=["*"],
                )

        return filters


    ########################
    ### Event Processing ###
    ########################
    async def _process_orderbook_update(self, update: dict, block_time: str|None):
        """
        Process single orderbook update with sequence validation

        """

        #  Chain
        # {'seq': '126933',
        #   'orderbook': {
        #                   'marketId': '0xf12a831d2901a1c1130fb81c88c3b443c62be3cd14547625f2db8a5f2b187d2c',
        #                   'buyLevels': [
        #                                   {'p': '451920000000000000000000000', 'q': '1248000000000000000'},
        #                                   {'p': '453840000000000000000000000', 'q': '4205000000000000000'},
        #                                   {'p': '455780000000000000000000000', 'q': '14169000000000000000'},
        #                                   {'p': '455820000000000000000000000', 'q': '0'}
        #                               ],
        #                   'sellLevels': [
        #                                   {'p': '458170000000000000000000000', 'q': '10324000000000000000'},
        #                                   {'p': '458510000000000000000000000', 'q': '0'},
        #                                   {'p': '460120000000000000000000000', 'q': '3064000000000000000'},
        #                                   {'p': '460460000000000000000000000', 'q': '0'},
        #                                   {'p': '462080000000000000000000000', 'q': '909000000000000000'},
        #                                   {'p': '462420000000000000000000000', 'q': '0'}
        #                               ]
        #               }
        # }
        start_time = time.time()
        try:
            # Log input data for debugging
            self.logger.debug(f"Processing orderbook update: market_id={update['orderbook']['marketId']}, seq={update['seq']}")

            self.logger.debug(f"Chain update: {update}")
            orderbook_data = update['orderbook']
            market_id = orderbook_data["marketId"]
            sequence = int(update["seq"])
            market_ticker = self.mediator.id_to_ticker[market_id]
            self.logger.debug(f"Chain update: {market_ticker}, sequence {sequence}")

            # Validate sequence
            market: Market = self.mediator.markets[market_id]
            last_sequence = market.orderbook.sequence
            if last_sequence:
                orderbook = self.mediator.markets[market_id].orderbook
                if sequence <= last_sequence:
                    orderbook.is_healthy = False # Changed orderbook status to False to indicate orderbook is inaccurate
                    self.logger.warning(f"Out of order sequence for {market_ticker}: got {sequence}, last was {last_sequence}. Ignored update")
                    return
                elif sequence > last_sequence + 1:
                    orderbook.is_healthy = False # Changed orderbook status to False to indicate orderbook is inaccurate
                    gap_size = sequence - last_sequence - 1
                    self.logger.warning(f"Sequence gap detected for {market_ticker}: missing {gap_size}/{self.large_gap_threshold} updates")
                    notification = self._transform_orderbook_update(market, update, block_time)
                    priority_queue = self._update_buffer[market_id]
                    if priority_queue.full():
                        priority_queue.get_nowait()
                    priority_queue.put_nowait((sequence, notification))
                    self.logger.info(f"Pushed {market_ticker} notification {sequence} to update buffer.")
                    if gap_size >= self.large_gap_threshold:
                        self.logger.debug("Trying to request a new snapshot")
                        await self._condition.notify(market_id)
                else:
                    if not orderbook.is_healthy: # Changed orderbook status to False to indicate orderbook is inaccurate
                        orderbook.is_healthy = True
                    self.logger.info(f"Consecutive orderbook sequence: {last_sequence} -> {sequence}")
                    notification = self._transform_orderbook_update(market, update, block_time)
                    await self.mediator.notify(notification)
                    self.logger.debug(f"Sent {market_ticker} {notification.event} {sequence} to mediator")
                    orderbook = self.mediator.markets[market_id].orderbook
                    self._sequence_tracker[market_id] = orderbook.sequence
        except Exception as e:
            market_id = update.get('orderbook', {}).get('marketId', 'unknown')
            sequence = update.get('seq', 'unknown')
            error_details = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
            self.logger.error(
                f"Error processing orderbook update for market {market_id}, sequence {sequence}: {e}\n"
                f"Update data: {update}\n"
                f"Traceback:\n{error_details}"
            )
            raise
        finally:
            elapsed_time = time.time() - start_time
            self.logger.debug(f"Orderbook update processing took {elapsed_time:.4f} seconds")


    async def _process_oracle_price(self, oracle_data: dict[str,str], block_height: int, block_time: datetime):
        """
        Process oracle price update with improved type handling for different oracle sources.

        Args:
            oracle_data: Oracle price data from chain
            block_height: Current block height
            block_time: Current block time
        """
        # Extract basic oracle data
        original_symbol = oracle_data.get("symbol")
        price_str = oracle_data.get("price")
        oracle_type = oracle_data.get("type")

        if not original_symbol or not price_str or not oracle_type:
            self.logger.warning(f"Skipping incomplete price data: {oracle_data}")
            return

        try:
            # Map full symbol (e.g., BTC/USD) to internal symbol (e.g., BTC)
            decimals_dict = ORACLE_DECIMALS.get(oracle_type, {})
            decimals = decimals_dict.get(original_symbol)

            if decimals is None:
                self.logger.debug(f"Missing decimal for {original_symbol} in oracle, type {oracle_type}, oracle: {price_str} using default 18, {Decimal(price_str) / Decimal(10**18)}")
                decimals = 18

            price = Decimal(price_str) / Decimal(10 ** decimals)
            self.logger.debug(f"Processed {oracle_type} oracle price for {original_symbol} with {decimals} decimals: {price_str} -> {price}")

            if oracle_type == "pyth":
                internal_symbol = PYTH_FEED_ID_TO_SYMBOL.get(original_symbol, original_symbol).split('.')[-1].split('/')[0]
            elif oracle_type == "stork":
                internal_symbol = STORK_ORACLE_SYMBOL.get(original_symbol, original_symbol)
            elif oracle_type == "provider":
                internal_symbol = PROVIDER_ORACLE_SYMBOL.get(original_symbol, original_symbol)
            else:
                self.logger.warning(f"Oracle type {oracle_type} is not available.")
                return

            if internal_symbol in self.oracle_symbols:
                if decimals is None:
                    self.logger.warning(f"Missing decimal for {original_symbol} in oracle, type {oracle_type}, oracle: {price_str} using default 18, {Decimal(price_str) / Decimal(10**18)}")
                    decimals = 18

                price = Decimal(price_str) / Decimal(10 ** decimals)
                self.logger.debug(f"Processed {oracle_type} oracle price for {original_symbol} with {decimals} decimals: {price_str} -> {price}")

                if price <= 0:
                    self.logger.warning(f"Invalid non-positive price for {original_symbol}: {price}")
                    return

                notification = Notification(
                    event=Event.ORACLE_UPDATE,
                    data={
                        "symbol": internal_symbol,
                        "price": price,
                        "type": oracle_type,
                        "block_height": block_height,
                        "timestamp": block_time,
                    }
                )
                self.logger.debug(f"Created oracle update notification: {notification.data}")
                await self.mediator.notify(notification)

        except (ValueError, TypeError, ArithmeticError) as e:
            self.logger.error(f"Error processing price for {original_symbol}: {e}", exc_info=True)
            self.logger.debug(f"Problematic price data: {oracle_data}")

    async def _process_bank_balances(self, bank_balance: dict):
        # BankBalances:
        #    [{'account': 'inj address',
        #      'balances': [{'amount': 'amount', 'denom': 'inj'}]},
        #     {'account': 'inj address',
        #      'balances': [{'amount': 'amount', 'denom': 'inj'}]}]
        for balance in bank_balance["balances"]:
            denom = balance["denom"]
            if denom not in self.denom_decimals:
                self.logger.error(f"Unknown token denom: {denom}")
                continue

            notification = Notification(
                event=Event.BALANCE_UPDATE,
                data={
                    "account_address": bank_balance["account"],
                    "denom": denom,
                    "amount": Decimal(balance["amount"]) / self.denom_decimals[denom]
                }
            )
            self.logger.debug(f"Created balance update notification: {notification.data}")
            await self.mediator.notify(notification)

    async def _process_subaccount_deposits(self, subaccount_deposit: dict):
        # SubaccountDeposits:
        # { 'subaccountDeposits': [{'deposits': [{'denom': 'peggy0xdAC17F958D2ee523a2206206994597C13D831ec7',
        #                'deposit': {'availableBalance': '69582949030489413943869142156',
        #                            'totalBalance': '90187013560489413943869144762'}}],
        #   'subaccountId': 'subaccount id'},
        #  {'deposits': [{'denom': 'peggy0xdAC17F958D2ee523a2206206994597C13D831ec7',
        #                'deposit': {'availableBalance': '69582949030489413943869142156',
        #                            'totalBalance': '90187013560489413943869144762'}}],
        #   'subaccountId': 'subaccount id'}
        # ]}
        for deposit in subaccount_deposit["deposits"]:
            denom = deposit["denom"]
            if denom not in self.denom_decimals:
                self.logger.error(f"Unknown token denom: {denom}")
                continue

            notification = Notification(
                event=Event.DEPOSIT_UPDATE,
                data={
                    "subaccount_id": subaccount_deposit["subaccountId"],
                    "denom": denom,
                    "available": Decimal(deposit["deposit"]["availableBalance"]) / self.denom_decimals[denom],
                    "total": Decimal(deposit["deposit"]["totalBalance"]) / self.denom_decimals[denom],
                }
            )
            await self.mediator.notify(notification)

    async def _process_positions(self, position: dict):
        # Position
        # [{'cumulativeFundingEntry': '1165709584725390947363969',
        #  'entryPrice': '11416356429355375178053351',
        #  'isLong': False,
        #  'margin': '7535349259299236781936632111',
        #  'marketId': 'market id',
        #  'quantity': '1979750000000000000000',
        #  'subaccountId': 'subaccount id'},
        #
        #  {'cumulativeFundingEntry': '1165709584725390947363969',
        #  'entryPrice': '11416356429355375178053351',
        #  'isLong': False,
        #  'margin': '7535349259299236781936632111',
        #  'marketId': 'market id',
        #  'quantity': '1979750000000000000000',
        #  'subaccountId': 'subaccound id'}]
        market = self.mediator.markets[position["marketId"]]
        if isinstance(market.market, DerivativeMarket):
            notification = Notification(
                event=Event.POSITION_UPDATE,
                data={
                    "subaccount_id": position["subaccountId"],
                    "market_id": position["marketId"],
                    "cumulative_funding_entry": Decimal(market.market.notional_from_extended_chain_format(Decimal(position["cumulativeFundingEntry"]))),
                    "entry_price": Decimal(market.market.price_from_extended_chain_format(Decimal(position["entryPrice"]))),
                    "margin": Decimal(market.market.margin_from_extended_chain_format(Decimal(position["margin"]))),
                    "quantity": Decimal(market.market.quantity_from_extended_chain_format(Decimal(position["quantity"]))),
                    "is_long": position["isLong"],
                }
            )
            await self.mediator.notify(notification)

    async def _process_spot_orders(self, spot_order: dict):
        # SpotOrders
        # [{'cid': 'cid',
        #  'order': {'marketId': 'market id',
        #            'order': {'fillable': '448000000000000000000000000000000000',
        #                      'orderHash': 'order hash', # need to decode,
        #                      'orderInfo': {'cid': 'cid',
        #                                    'feeRecipient': 'fee recipient',
        #                                    'price': '2682500000',
        #                                    'quantity': '448000000000000000000000000000000000',
        #                                    'subaccountId': 'subaccount id'},
        #                      'orderType': 'SELL_PO',
        #                      'triggerPrice': '0'}},
        #  'orderHash': 'order hash',
        #  'status': 'Cancelled'},
        #
        #  {'cid': 'cid',
        #  'order': {'marketId': 'market id',
        #            'order': {'fillable': '448000000000000000000000000000000000',
        #                      'orderHash': 'order hash',
        #                      'orderInfo': {'cid': '2160076097640586499',
        #                                    'feeRecipient': 'fee recipient',
        #                                    'price': '2682500000',
        #                                    'quantity': '448000000000000000000000000000000000',
        #                                    'subaccountId': 'subaccount id'},
        #                        'orderType': 'SELL_PO',
        #                        'triggerPrice': '0'}},
        #  'status': 'Cancelled'}]
        self.logger.debug(f"Processing the spot order with order hash: {spot_order['order']['order']['orderHash']}")
        market_id = spot_order["order"]["marketId"]
        market = self.mediator.markets[market_id]
        order_type_str = spot_order["order"]["order"]["orderType"]
        order_type = Side.BUY if order_type_str.startswith("BUY") or order_type_str.endswith("BUY") else Side.SELL
        notification = Notification(
            event=Event.SPOT_ORDER_UPDATE,
            data={
                "cid": spot_order["cid"],
                "market_id": market_id,
                "fillable": Decimal(market.market.quantity_from_extended_chain_format(Decimal(spot_order["order"]["order"]["fillable"]))),
                "fee_recipient": spot_order["order"]["order"]["orderInfo"]["feeRecipient"],
                "price": Decimal(market.market.price_from_extended_chain_format(Decimal(spot_order["order"]["order"]["orderInfo"]["price"]))),
                "quantity": Decimal(market.market.quantity_from_extended_chain_format(Decimal(spot_order["order"]["order"]["orderInfo"]["quantity"]))),
                "subaccount_id": spot_order["order"]["order"]["orderInfo"]["subaccountId"],
                "order_type": order_type,
                "trigger_price": Decimal(market.market.price_from_extended_chain_format(Decimal(spot_order["order"]["order"]["triggerPrice"]))),
                "order_hash": order_hash_from_chain_format(spot_order["order"]["order"]["orderHash"]),
                "status": spot_order["status"],
            }
        )
        await self.mediator.notify(notification)

    async def _process_derivative_orders(self, derivative_order: dict):
        # DerivativeOrders
        # [{'cid': 'cid',
        #  'order': {'isMarket': False,
        #            'marketId': 'market id',
        #            'order': {'fillable': '51270000000000000000',
        #                      'margin': '3230702000000000000000000000',
        #                      'orderHash': 'order hash',
        #                      'orderInfo': {'cid': 'cid',
        #                                    'feeRecipient': 'fee recipient',
        #                                    'price': '614460000000000000000000000',
        #                                    'quantity': '51270000000000000000',
        #                                    'subaccountId': 'subaccount id'},
        #                      'orderType': 'BUY_PO',
        #                      'triggerPrice': ''}},
        #  'status': 'Cancelled'}]
        self.logger.debug(f"Processing the derivative order with order hash: {derivative_order['order']['order']['orderHash']}")
        market = self.mediator.markets[derivative_order["order"]["marketId"]]
        order_type_str = derivative_order["order"]["order"]["orderType"]
        order_type = Side.BUY if order_type_str.startswith("BUY") or order_type_str.endswith("BUY") else Side.SELL
        notification = Notification(
            event=Event.DERIVATIVE_ORDER_UPDATE,
            data={
                "cid": derivative_order["cid"],
                "market_id": derivative_order["order"]["marketId"],
                "fillable": Decimal(market.market.quantity_from_extended_chain_format(Decimal(derivative_order["order"]["order"]["fillable"]))),
                "margin": Decimal(market.market.margin_from_extended_chain_format(Decimal(derivative_order["order"]["order"]["margin"]))),
                "fee_recipient": derivative_order["order"]["order"]["orderInfo"]["feeRecipient"],
                "price": Decimal(market.market.price_from_extended_chain_format(Decimal(derivative_order["order"]["order"]["orderInfo"]["price"]))),
                "quantity": Decimal(market.market.quantity_from_extended_chain_format(Decimal(derivative_order["order"]["order"]["orderInfo"]["quantity"]))),
                "subaccount_id": derivative_order["order"]["order"]["orderInfo"]["subaccountId"],
                "order_type": order_type,
                "trigger_price": Decimal(market.market.price_from_extended_chain_format(Decimal(derivative_order["order"]["order"]["triggerPrice"]))),
                "order_hash": order_hash_from_chain_format(derivative_order["order"]["order"]["orderHash"]),
                "status": derivative_order["status"],
            }
        )
        await self.mediator.notify(notification)

    async def _process_spot_trades(self, spot_trade: dict):
        # SpotTrades
        # [{'cid': 'cid',
        #  'executionType': 'LimitMatchRestingOrder',
        #  'fee': '-1604700000000000000000',
        #  'feeRecipientAddress': 'fee recipient',
        #  'isBuy': False,
        #  'marketId': 'market id',
        #  'orderHash': 'order hash',
        #  'price': '2674500000',
        #  'quantity': '10000000000000000000000000000000000',
        #  'subaccountId': 'subaccount id',
        #  'tradeId': 'trader id'},
        #
        #  {'cid': 'cid',
        #  'executionType': 'LimitMatchRestingOrder',
        #  'fee': '-1604700000000000000000',
        #  'feeRecipientAddress': 'fee recipient',
        #  'isBuy': False,
        #  'marketId': 'market id ',
        #  'orderHash': 'order hash',
        #  'price': '2674500000',
        #  'quantity': '10000000000000000000000000000000000',
        #  'subaccountId': 'subaccount id',
        #  'tradeId': 'trade id'}]
        self.logger.debug(f"Processing the spot trade with order hash: {spot_trade['orderHash']}")
        market = self.mediator.markets[spot_trade["marketId"]]
        notification = Notification(
            event=Event.SPOT_TRADE_UPDATE,
            data={
                "cid": spot_trade["cid"],
                "order_hash": spot_trade["orderHash"],
                "subaccount_id": spot_trade["subaccountId"],
                "market_id": spot_trade["marketId"],
                "side": Side.BUY if spot_trade["isBuy"] else Side.SELL,
                "execution_type": spot_trade["executionType"],
                "fee": Decimal(market.market.notional_from_extended_chain_format(Decimal(spot_trade["fee"]))),
                "fee_recipient_address": spot_trade["feeRecipientAddress"],
                "price": Decimal(market.market.price_from_extended_chain_format(Decimal(spot_trade["price"]))),
                "quantity": Decimal(market.market.quantity_from_extended_chain_format(Decimal(spot_trade["quantity"]))),
                "trade_id": spot_trade["tradeId"]
            }
        )
        await self.mediator.notify(notification)

    async def _process_derivative_trades(self, derivative_trade: dict):
        # DerivativeTrades
        # [{'cid': 'cid',
        #  'executionType': 'LimitMatchNewOrder',
        #  'fee': '621918900000000000000',
        #  'feeRecipientAddress': 'fee recipient',
        #  'isBuy': True,
        #  'marketId': 'market id',
        #  'orderHash': 'order hash',
        #  'payout': '424017498578031916218037',
        #  'positionDelta': {'executionMargin': '425227588888888888888889',
        #                    'executionPrice': '22291000000000000000000000',
        #                    'executionQuantity': '186000000000000000',
        #                    'isLong': True},
        #  'subaccountId': 'subaccount id',
        #  'tradeId': 'trade id'},
        #
        #  {'cid': 'cid',
        #  'executionType': 'LimitMatchNewOrder',
        #  'fee': '621918900000000000000',
        #  'feeRecipientAddress': 'fee recipient',
        #  'isBuy': True,
        #  'marketId': 'market id',
        #  'orderHash': 'order hash',
        #  'payout': '424017498578031916218037',
        #  'positionDelta': {'executionMargin': '425227588888888888888889',
        #                    'executionPrice': '22291000000000000000000000',
        #                    'executionQuantity': '186000000000000000',
        #                    'isLong': True},
        #  'subaccountId': 'subaccount id',
        #  'tradeId': 'trade id'}]
        self.logger.debug(f"Processing the derivative trade with order hash: {derivative_trade['orderHash']}")
        market = self.mediator.markets[derivative_trade["marketId"]]
        if isinstance(market.market, DerivativeMarket):
            notification = Notification(
                event=Event.DERIVATIVE_TRADE_UPDATE,
                data={
                    "cid": derivative_trade["cid"],
                    "execution_type": derivative_trade["executionType"],
                    "fee": market.market.notional_from_extended_chain_format(Decimal(derivative_trade["fee"])),
                    "fee_recipient_address": derivative_trade["feeRecipientAddress"],
                    "side": Side.BUY if derivative_trade["isBuy"] else Side.SELL,
                    "market_id": derivative_trade["marketId"],
                    "order_hash": derivative_trade["orderHash"],
                    "payout": market.market.notional_from_extended_chain_format(Decimal(derivative_trade["payout"])),
                    "execution_margin": market.market.margin_from_extended_chain_format(
                        Decimal(derivative_trade["positionDelta"]["executionMargin"])
                    ),
                    "execution_price": market.market.price_from_extended_chain_format(Decimal(derivative_trade["positionDelta"]["executionPrice"])),
                    "execution_quantity": market.market.quantity_from_extended_chain_format(
                        Decimal(derivative_trade["positionDelta"]["executionQuantity"])
                    ),
                    "is_long": derivative_trade["positionDelta"]["isLong"],
                    "subaccount_id": derivative_trade["subaccountId"],
                    "trader_id": derivative_trade["tradeId"],
                }
            )
            await self.mediator.notify(notification)

    ######################
    ### Helper Methods ###
    ######################

    async def _cleanup_stream(self, context: Dict, error_type: str):
        """
        Clean up resources after stream failure
        """
        if self._stream_task and not self._stream_task.done():
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                self.logger.info(f"Cleaned up stream resources after {error_type} error.")
            finally:
                self._stream_task = None

    async def _cleanup_snapshot(self, context: Dict, error_type: str):
         """
         Clean up resources after stream failure
         """
         if self._snapshot_task and not self._snapshot_task.done():
             self._snapshot_task.cancel()
             try:
                 await self._snapshot_task
             except asyncio.CancelledError:
                 self.logger.info(f"Cleaned up snapshot resources after {error_type} error.")
             finally:
                 self._snapshot_task= None

    async def _validate_sequence(self, update: Dict) -> bool:
        """Add sequence validation before processing orderbook updates"""
        market_id = update["orderbook"]["marketId"]
        sequence = int(update["seq"])

        last_sequence = self._sequence_tracker.get(market_id)
        if last_sequence:
            if sequence <= last_sequence:
                self.logger.warning(f"Out of order sequence for {market_id}: got {sequence}, last was {last_sequence}")
                return False
            if sequence > last_sequence + 1:
                self.logger.warning(f"Sequence gap detected for {market_id}: gap of {sequence - last_sequence - 1}")
                orderbook = self.mediator.markets[market_id].orderbook
                orderbook.is_healthy = False

        self._sequence_tracker[market_id] = sequence
        return True

    async def _request_orderbook_snapshot(self, market_id: str):
        """Request full orderbook snapshot when significant gaps detected"""
        request_id = str(uuid.uuid4())[:8]  # Short unique ID
        try:
            self.logger.info(f"[{request_id}] Starting orderbook snapshot request for {market_id} [{self.mediator.id_to_ticker[market_id]}]")
            self.logger.info(f"State before {self.mediator.id_to_ticker[market_id]} snapshot request, sequence {self._sequence_tracker.get(market_id)}")
            market_ticker = self.mediator.id_to_ticker[market_id]
            # Fetch fresh orderbook
            response = None
            if market_id in self.mediator.spot_market_ids:
                self.logger.info(f"Requesting spot orderbook snapshot for {market_id} [{market_ticker}]")
                response = await self.retry_handler.execute_with_retry(
                    operation=self.client.fetch_spot_orderbooks_v2,
                    market_ids=[market_id],
                    context={}
                )
            elif market_id in self.mediator.perp_market_ids:
                self.logger.info(f"Requesting derivative orderbook snapshot for {market_id} [{market_ticker}]")
                response = await self.retry_handler.execute_with_retry(
                    operation=self.client.fetch_derivative_orderbooks_v2,
                    market_ids=[market_id],
                    context={}
                )
            else:
                raise ValueError(f"Market {market_id}[{market_ticker}] is not in spot or derivative markets of interest.")

            if not response or "orderbooks" not in response:
                raise ValueError(f"Invalid response for {market_id}[{market_ticker}]")

            # Process snapshot
            for orderbook_data in response["orderbooks"]:
                if orderbook_data["marketId"] == market_id:
                    old_sequence = self._sequence_tracker.get(market_id)
                    new_sequence = int(orderbook_data['orderbook']["sequence"])
                    self.logger.info(f"Update {market_ticker} sequence: {old_sequence} -> {new_sequence}")
                    orderbook = self.mediator.markets[market_id].orderbook
                    # tmp_orderbook = self._transform_indexer_orderbook_update(orderbook_data['orderbook'])
                    tmp_orderbook = self._transform_indexer_orderbook_update(orderbook_data['orderbook'], market_id)
                    orderbook.batch_update_orderbook(updates=tmp_orderbook, sequence=new_sequence, market_ticker=market_ticker, force_update=True)
                    self._sequence_tracker[market_id] = new_sequence
                else:
                    raise ValueError(f"Market {self.mediator.id_to_ticker[market_id]} not found in response")
            self.logger.info(f"State after {self.mediator.id_to_ticker[market_id]} snapshot request, sequence {self._sequence_tracker.get(market_id)}")
            self.logger.info(f"[{request_id}] Completed orderbook snapshot request")
        except Exception as e:
            self.logger.error(f"Failed to fetch orderbook snapshot: {e}")
            self.logger.error(f"[{request_id}] Failed orderbook snapshot request: {e}")
            raise

    def _transform_indexer_orderbook_update(self, orderbook: dict, market_id: str) -> dict[str,list[dict[str, Decimal]]]:
        self.logger.debug(f"market_id: {market_id} ")
        market = self.mediator.markets[market_id].market
        self.logger.debug(f"market: {market} ")
        asks = [{"p": market.price_from_chain_format(Decimal(ask['price'])), "q": market.quantity_from_chain_format(Decimal(ask['quantity']))} for ask in orderbook['sells']]
        bids = [{"p": market.price_from_chain_format(Decimal(buy['price'])), "q": market.quantity_from_chain_format(Decimal(buy['quantity']))} for buy in orderbook['buys']]
        return {"asks": asks, "bids": bids}

    def _transform_orderbook_update(self, market: Market, update:dict, block_time:str|None)->Notification:
        self.logger.debug(f"Chain update: {update}")
        orderbook_data = update['orderbook']
        market_id = orderbook_data["marketId"]
        sequence = int(update["seq"])
        market_ticker = self.mediator.id_to_ticker[market_id]
        self.logger.debug(f"Chain update: {market_ticker}, sequence {sequence}")

        # Process the update
        self.logger.debug(f"Got orderbook update: {market_ticker}")
        orderbook = update_order_book_to_human_readable(update, market.market)
        self.logger.debug(f"Human readable order book: {orderbook}")

        valid_buys = []
        valid_sells = []

        for level in orderbook["orderbook"]["buyLevels"]:
            if level["p"] % market.min_price_tick == 0:
                valid_buys.append({"p": level["p"], "q": level["q"]})
            else:
                self.logger.warning(f"Skipping bid with invalid tick size - price: {level['p']}, tick: {market.min_price_tick}")

        for level in orderbook["orderbook"]["sellLevels"]:
            if level["p"] % market.min_price_tick == 0:
                valid_sells.append({"p": level["p"], "q": level["q"]})
            else:
                self.logger.warning(f"Skipping ask with invalid tick size - price: {level['p']}, tick: {market.min_price_tick}")

        return Notification(
            event = Event.ORDERBOOK_UPDATE,
            data={
                "market_id": market_id,
                "updates": {
                    "bids": valid_buys,
                    "asks": valid_sells,
                },
                "sequence": sequence,
                "block_time": block_time,
            },
        )
