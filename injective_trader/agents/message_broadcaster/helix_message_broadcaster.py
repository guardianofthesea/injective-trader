from __future__ import annotations
import json
from typing import Coroutine, Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
from decimal import Decimal
import asyncio
from grpc import RpcError
from abc import ABC, abstractmethod
import time
from datetime import datetime, timedelta

from pyinjective.constant import GAS_FEE_BUFFER_AMOUNT, GAS_PRICE
from pyinjective.transaction import Transaction
from pyinjective import AsyncClient
from pyinjective.core.network import Network
from pyinjective.composer import Composer

from injective_trader.domain.account.account import Account
from injective_trader.domain.granteepool import GranteePool, GranteeState
from injective_trader.core.component import Component
from injective_trader.utils.enums import Event
from injective_trader.utils.helpers import return_orders_states_after_sim, get_correct_sequence_number
from injective_trader.utils.retry import get_retry_handler, RetryHandler
from injective_trader.utils.message_factory import *
from uuid import uuid4

######################
### MessageBatcher ###
######################

@dataclass
class BatchConfig:
    """Configuration for message batching"""
    max_batch_size: int = 15  # Maximum number of messages per batch
    max_gas_limit: int = 5_000_000  # Maximum gas per batch
    max_batch_delay: float = 0.5  # Maximum seconds to wait for batch
    min_batch_size: int = 3  # Minimum messages to trigger immediate send

class MessageBatcher:
    """Handles message batching logic"""

    def __init__(self, logger, config: Dict):
        self.logger = logger
        self.batch_config = BatchConfig(
            max_batch_size=config.get("MaxBatchSize", 15),
            max_gas_limit=config.get("MaxGasLimit", 5_000_000),
            max_batch_delay=config.get("MaxBatchDelay", 0.5),
            min_batch_size=config.get("MinBatchSize", 3)
        )
        self.pending_msgs: List = []
        self.batch_start_time: Optional[float] = None

    def add_message(self, msg: Dict) -> bool:
        """Add message to pending batch"""
        self.logger.debug(f"Adding message to batch: current size {len(self.pending_msgs)}")
        if not self.pending_msgs:
            self.batch_start_time = asyncio.get_event_loop().time()
            self.logger.debug(f"Batch start time set to {self.batch_start_time}")

        self.pending_msgs.append(msg)
        return self.should_send_batch()

    def should_send_batch(self) -> bool:
        """Check if batch should be sent"""
        self.logger.debug(f"Checking if batch should be sent: size {len(self.pending_msgs)}")
        if len(self.pending_msgs) >= self.batch_config.max_batch_size:
            return True

        self.logger.debug(f"Checking batch timing: {len(self.pending_msgs)} >= {self.batch_config.min_batch_size} and {self.batch_start_time}")
        if len(self.pending_msgs) >= self.batch_config.min_batch_size and self.batch_start_time:
            current_time = asyncio.get_event_loop().time()
            self.logger.debug(f"Current time: {current_time}, batch start time: {self.batch_start_time}, max delay: {self.batch_config.max_batch_delay}")
            if current_time >= self.batch_config.max_batch_delay + self.batch_start_time :
                return True
        else:
            self.logger.debug(f"len {len(self.pending_msgs)} >= {self.batch_config.min_batch_size} and batch_start_time: {self.batch_start_time}")

        return False

    def get_next_batch(self) -> List:
        """Get and clear current batch"""
        batch = self.pending_msgs
        self.pending_msgs = []
        self.batch_start_time = None
        return batch


##########################
### MessageBroadcaster ###
##########################

@dataclass
class BroadcastResult:
    """
    Enhanced transaction result with detailed status information
    """
    success: bool
    gas_info: Optional[Dict] = None
    error: Optional[str] = None
    tx_hash: Optional[str] = None
    sequence: Optional[int] = None
    authz_validated: bool = False
    simulation_passed: bool = False

    @property
    def gas_wanted(self) -> int:
        return self.gas_info.get("gas_wanted", 0) if self.gas_info else 0

    @property
    def gas_fee(self) -> Decimal:
        return Decimal(self.gas_info.get("gas_fee", 0)) if self.gas_info else Decimal("0")

class HelixMessageBroadcaster(Component):
    """
    Handles message broadcasting with support for:
    - Message batching
    - Authz delegation
    - Multiple grantees
    - Retry handling
    - Transaction simulation
    """
    TIMEOUT_BLOCK_HEIGHT = 30

    def __init__(self, logger, composer_base, debug: bool = False):
        super().__init__(logger, composer_base)
        self.name = "MessageBroadcaster"
        self.debug = debug

        # Core components
        self.pending_broadcasts = asyncio.Queue()
        self.error_codes: Dict[str, str] = {}
        self.grantee_pools: Dict[str, GranteePool] = {}
        self.message_factory = HelixMessageFactory(self.logger, self.composer)
        self.batcher: Optional[MessageBatcher] = None

        # Transaction processors
        self.direct_processor: DirectTransactionProcessor|None = None
        self.authz_processors: Dict[str, AuthzTransactionProcessor] = {}

        # Background tasks
        self._broadcast_task: Optional[asyncio.Task] = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._batch_task: Optional[asyncio.Task] = None
        self._active = True
        self._shutdown_event = asyncio.Event()

    async def initialize(self, **kwargs):
        """Initialize broadcaster with configuration"""
        try:
            # Get configuration from kwargs
            self.config = kwargs.get("config", {})
            await self.client.sync_timeout_height()
            self.mediator.block_height =  self.client.timeout_height + self.TIMEOUT_BLOCK_HEIGHT
            self.logger.debug(f"Initializing block timeout height {self.mediator.block_height}")
            # Load error codes
            error_codes_path = self.config.get("ErrorCodesJson", "config/error_codes.json")
            try:
                with open(error_codes_path, 'r') as f:
                    self.error_codes = json.load(f)
                self.logger.info(f"Loaded error codes from {error_codes_path}")
            except Exception as e:
                self.logger.error(f"Failed to load error codes from {error_codes_path}: {e}")
                self.error_codes = {}

            # Setup retry handler
            retry_config = self.config.get("RetryConfig", {})
            self.retry_handler = get_retry_handler(
                self.logger,
                retry_config,
                "MessageBroadcaster"
            )

            # Initialize message batcher
            batch_config = self.config.get("Batch", {})
            self.batcher = MessageBatcher(self.logger, batch_config)

            # Initialize direct transaction processors
            self.direct_processor = DirectTransactionProcessor(
                self.logger,
                self.client,
                self.composer,
                self.network,
                self.retry_handler,
                self.debug
            )

            # Typed Authorization Messages
            authz_message_types = [
                # "/injective.exchange.v1beta1.MsgCreateSpotLimitOrder",
                # "/injective.exchange.v1beta1.MsgCreateSpotMarketOrder",
                # "/injective.exchange.v1beta1.MsgBatchCreateSpotLimitOrders",
                # "/injective.exchange.v1beta1.MsgCancelSpotOrder",
                # "/injective.exchange.v1beta1.MsgBatchCancelSpotOrders",
                # "/injective.exchange.v1beta1.MsgCreateDerivativeLimitOrder",
                # "/injective.exchange.v1beta1.MsgCreateDerivativeMarketOrder",
                # "/injective.exchange.v1beta1.MsgBatchCreateDerivativeLimitOrders",
                # "/injective.exchange.v1beta1.MsgCancelDerivativeOrder",
                # "/injective.exchange.v1beta1.MsgBatchCancelDerivativeOrders",
                "/injective.exchange.v1beta1.MsgBatchUpdateOrders"
            ]

            # Initialize grantee pools from AuthzPools configuration
            for pool_info in self.config.get("AuthzPools", []):
                granter = pool_info["granter"]
                grantees = pool_info["grantees"]

                if granter and grantees:
                    # Create grantee pool
                    pool_config = self.config.get("GranteePool", {})
                    pool = GranteePool(self.logger, granter, pool_config)

                    # Create grantee configurations
                    grantee_configs = []
                    for grantee in grantees:
                        # Fetch current account sequence
                        try:
                            account_data = await self.client.fetch_account(grantee)
                            initial_sequence = account_data.base_account.sequence
                        except Exception as e:
                            self.logger.warning(f"Failed to fetch sequence for grantee {grantee}: {e}")
                            initial_sequence = 1

                        # Check and setup grants if needed
                        missing_grant_types = []
                        grants = []
                        try:
                            for msg_type in authz_message_types:
                                grantee_response = await self.client.fetch_grants(
                                    granter=granter,
                                    grantee=grantee,
                                    msg_type_url=msg_type
                                )

                                if grantee_response and "grants" in grantee_response and grantee_response["grants"]:
                                    for grant in grantee_response["grants"]:
                                        if "expiration" in grant:
                                            expiration = grant["expiration"]
                                            grants.append({
                                                "Type": msg_type,
                                                "Expiration": expiration
                                            })
                                            self.logger.info(f"Found grant for {msg_type} expiring at {expiration}")
                                else:
                                    missing_grant_types.append(msg_type)
                        except Exception as e:
                            self.logger.warning(f"Failed to fetch grants for {grantee}: {e}")
                            missing_grant_types.append(msg_type)

                        if missing_grant_types:
                            grant_result = await self.setup_authz_grants(granter, grantee, missing_grant_types)
                            for msg_type, result in grant_result.items():
                                if result and result["success"]:
                                    self.logger.info(f"Successfully granted {msg_type} from {granter} to {grantee}")
                                    grants.append({
                                        "Type": msg_type,
                                        "Expiration": result.get("expiration", "2025-12-31T00:00:00Z")
                                    })
                                else:
                                    self.logger.error(f"Failed to grant {msg_type} from {granter} to {grantee}: {result.get('error') if result else result}")

                        if not grants:
                            self.logger.warning(f"No grants found for grantee {grantee}, using default expiration")
                            grants = [
                                {
                                    "Type": "/injective.exchange.v1beta1.MsgBatchUpdateOrders",
                                    "Expiration": "2025-12-31T00:00:00Z"
                                }
                            ]

                        grantee_configs.append({
                            "Address": grantee,
                            "InitialSequence": initial_sequence,
                            "Grants": grants
                        })

                    # Initialize grantees in the pool
                    await pool.initialize_grantees(grantee_configs)
                    self.grantee_pools[granter] = pool

                    # Initialize authz processor with different pools
                    self.authz_processors[granter] = AuthzTransactionProcessor(
                        self.logger,
                        self.client,
                        self.composer,
                        self.network,
                        self.retry_handler,
                        pool,
                        self.debug
                    )

                    self.logger.info(f"Initialized grantee pool for granter {granter} with {len(grantees)} grantees")

            self.logger.info(f"MessageBroadcaster initialized with {len(self.grantee_pools)} grantee pools")

        except Exception as e:
            self.logger.error(f"Failed to initialize MessageBroadcaster: {e}")
            raise

    async def run(self, **kwargs):
        """Main component run loop"""
        try:
            # Start background tasks
            self._broadcast_task = asyncio.create_task(self._process_broadcasts(), name="BroadcastTask")
            self._refresh_task = asyncio.create_task(self._refresh_grants_loop(), name="RefreshTask")
            self._batch_task = asyncio.create_task(self._process_batch_queue(), name="BatchTask")

            self.logger.info("MessageBroadcaster background tasks started")

            # Wait for shutdown signal instead of polling
            await self._shutdown_event.wait()

        except asyncio.CancelledError:
            self.logger.info("MessageBroadcaster run loop cancelled")
            self._active = False
            raise
        finally:
            await self._cleanup()

    async def receive(self, event: Event, data: Dict):
        """
        Handle incoming broadcast requests

        data: Dict
        - result
        - trading_mode: "direct" or "authz"
        - trading_info: trading_account address or granter + grantees addresses
        - trading_account: provide Account data (trading_account or granter_account + grantee_accounts)
        """

        cid = str(uuid4())
        self.logger.debug(f"Received broadcast request cid: [{cid}]")

        if event != Event.BROADCAST:
            raise ValueError(f"Unexpected event type: {event}")

        try:
            result: StrategyResult = data["result"]
            self.logger.debug(f"Received strategy result for broadcast, cid: [{cid}], result: {result}")
            trading_mode = data.get("trading_mode", "direct")
            trading_info = data.get("trading_info", {})
            account_data = data.get("account_data", {})
            broadcast_info = {
                "trading_mode": data["trading_mode"],
                "trading_info": data["trading_info"]
            }
            block_height = self.mediator.block_height
            self.logger.debug(f"{block_height} + {self.TIMEOUT_BLOCK_HEIGHT}")
            timeout_height = block_height + self.TIMEOUT_BLOCK_HEIGHT

            msgs = self.message_factory.create_messages(
                result=result,
                broadcast_info=broadcast_info
            )

            self.logger.debug(f"Generated {len(msgs)} messages for broadcast, cid: [{cid}], msgs: {msgs}")

            if not msgs:
                self.logger.warning(f"No message generated for broadcast, cid: [{cid}]")
                return

            t1 = datetime.now()
            context = {
                "trading_mode": trading_mode,
                "is_sync": True,
                "retry_count": 0,
                "max_retries": 3,
                "timeout_height": timeout_height,
                "receive_time": t1
            }
            self.logger.info(f"Prepared {len(msgs)} messages for broadcast in {trading_mode} mode at {t1}, cid: [{cid}]")
            # Build broadcast context based on trading mode
            if trading_mode == "direct":
                trading_account_address = trading_info.get("trading_account")
                trading_account: Account = account_data.get("trading_account")

                if not trading_account:
                    raise ValueError(f"Trading account not found for direct mode, cid: [{cid}]")

                # Check if account can sign transaction
                if not trading_account.can_sign_transaction():
                    raise ValueError(f"Trading account {trading_account_address} is in read-only mode and cannot sign transactions, cid: [{cid}]")

                context.update(
                    {
                        "account": trading_account,
                    }
                )

            elif trading_mode == "authz":
                granter_address = trading_info.get("granter")
                grantee_addresses = trading_info.get("grantees", [])

                granter_account = account_data.get("granter_account")
                grantee_accounts = account_data.get("grantee_accounts", {})

                if not granter_account:
                    raise ValueError(f"Granter account not found for authz mode. cid: [{cid}]")
                if not grantee_accounts:
                    raise ValueError(f"No grantee accounts found for authz mode. cid: [{cid}]")

                # Check if at least one grantee can sign
                signing_grantees = [addr for addr, acc in grantee_accounts.items()
                                    if acc.can_sign_transaction()]

                if not signing_grantees:
                    raise ValueError(f"None of the grantee accounts can sign transactions (all in read-only mode), cid: [{cid}]")

                # Check if missing some grantees that can sign
                if len(signing_grantees) < len(grantee_addresses):
                    read_only_grantees = set(grantee_addresses) - set(signing_grantees)
                    total_grantees = len(grantee_addresses)
                    signing_count = len(signing_grantees)

                    self.logger.warning(
                        f"⚠️ ATTENTION: Only {signing_count}/{total_grantees} grantees have private keys and can sign transactions. "
                        f"This reduces the pool of available grantees and may impact performance or resilience. "
                        f"For optimal performance, please provide private keys for all grantees: {read_only_grantees}, cid: [{cid}]"
                    )
                    self.logger.info(f"Using available signing grantees: {signing_grantees}, cid: [{cid}]")

                if granter_address not in self.grantee_pools:
                    raise ValueError(f"No grantee pool configured for granter {granter_address}, cid: [{cid}]")

                context.update(
                    {
                        "granter_account": granter_account,
                        "grantee_accounts": grantee_accounts,
                        "signing_grantees": signing_grantees,  # Add list of grantees that can sign
                        "authz_info": {
                            "granter": granter_address,
                            "grantees": grantee_addresses
                        },
                    }
                )

            else:
                return

            # Handle message batching if applicable
            self.logger.debug(f"batcher: {self.batcher} and should batch message: {self._should_batch_messages(msgs)}. cid: [{cid}]")
            if self._should_batch_messages(msgs) and self.batcher:
                batch_item = {
                    "msgs": msgs,
                    "context": context,
                    "cid": cid
                }

                if self.batcher.add_message(batch_item):
                    batch = self.batcher.get_next_batch()
                    await self._process_batch(batch)

            else:
                # Queue for direct processing
                await self.pending_broadcasts.put({
                    "msgs": msgs,
                    "context": context,
                    "cid": cid
                })
            t2 = datetime.now()
            self.logger.debug(f"context update latency: {t2 - t1} = {t2} - {t1}, cid: [{cid}]")

        except Exception as e:
            self.logger.error(f"Error processing broadcast request: {e}")
            raise

    async def _process_broadcasts(self):
        """Process queued broadcast requests"""
        cid = "N/A"
        while self._active:
            try:
                self.logger.debug(f"Processing broadcast request: {self.pending_broadcasts.qsize()} pending")
                request = await self.pending_broadcasts.get()
                cid = request.get("cid", "N/A")
                self.logger.info(f"Processing broadcast request: cid: {cid}")

                # If the queue has more items, drop all but the newest.
                dropped = 0
                while True:
                    try:
                        newer_request = self.pending_broadcasts.get_nowait()
                        # mark the item we’re NOT going to process as done
                        self.pending_broadcasts.task_done()
                        request = newer_request

                        newer_cid = newer_request.get("cid", "N/A")
                        self.logger.info(f"Coalesced stale request: replaced old cid: [{cid}] with new cid: [{newer_cid}]")
                        dropped += 1
                        cid = newer_cid
                    except asyncio.QueueEmpty:
                        break
                if dropped:
                    self.logger.warning(f"Coalesced {dropped} stale request(s); processing only the latest")

                msgs = request["msgs"]
                context = request["context"]
                t1 = context["receive_time"]
                t2 = datetime.now()
                latency = t2 - t1
                self.logger.info(f"Broadcast request latency: {latency} = {t2} - {t1}, cid: [{cid}]")

                # Process transaction using appropriate processor
                if context["trading_mode"] == "direct" and self.direct_processor:
                    result = await self.direct_processor.process_transaction(msgs, context)
                else:
                    granter = context["authz_info"]["granter"]
                    if granter in self.grantee_pools:
                        self.authz_processors[granter].grantee_pool = self.grantee_pools[granter]
                        result = await self.authz_processors[granter].process_transaction(msgs, context)
                    else:
                        raise Exception(f"Unknown granter: {granter}, cid: [{cid}]")
                t3 = datetime.now()
                latency = t3 - t1
                self.logger.info(f"Broadcast processing time: {latency} = {t3} - {t1}, cid: [{cid}]")

                if result:
                    if result['success']:
                        self.logger.critical(f"Broadcast result: success={result['success']}, tx_hash={result.get('tx_hash', 'N/A')}, cid: [{cid}]")
                    else:
                        trading_mode = context['trading_mode']
                        broadcast_address = context['grantee_state'].address if context['trading_mode'] == 'authz' else  context['account'].address

                        self.logger.warning(f"{trading_mode} broadcaster address: {broadcast_address}, " +
                                    f" Broadcast result: success={result['success']}, " +
                                    f"tx_hash={result.get('tx_hash', 'N/A')}, " +
                                    f"error={result.get('error', 'N/A')}, \n{result}, cid: [{cid}]")

            except asyncio.CancelledError:
                self.logger.info(f"Broadcast processing task cancelled, cid: [{cid}]")
                break
            except Exception as e:
                self.logger.error(f"Error processing broadcast: {e}, cid: [{cid}]")
            finally:
                if not self.pending_broadcasts.empty():
                    self.pending_broadcasts.task_done()

    async def _sync_account_sequences(self, context: Dict):
        """Synchronize account sequence numbers with the chain"""
        try:
            if context["trading_mode"] == "direct":
                account: Account = context["account"]
                #self.sync_account_sequence(account)
                account_data = await self.client.fetch_account(account.account_address)
                account.sequence = account_data.base_account.sequence
                self.logger.debug(f"Synced sequence for {account.account_address}: {account.sequence}")
            else:  # authz mode
                grantee_accounts = context["grantee_accounts"]
                ### TODO: We'll sync specific grantee sequences when selected by the processor
                pass

        except Exception as e:
            self.logger.warning(f"Failed to sync account sequences: {e}")

    async def _refresh_grants_loop(self):
        """Periodically refresh grant information"""
        while self._active:
            try:
                for pool in self.grantee_pools.values():
                    await pool.refresh_grants(self.client)
                refresh_interval = self.config.get("RefreshInterval", 300)
                await asyncio.sleep(refresh_interval)
            except asyncio.CancelledError:
                self.logger.info("Grant refresh task cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error refreshing grants: {e}")
                await asyncio.sleep(60)

    async def _process_batch_queue(self):
        """Process batched messages"""
        while self._active:
            try:
                if self.batcher and self.batcher.pending_msgs and self.batcher.should_send_batch():
                    batch = self.batcher.get_next_batch()
                    await self._process_batch(batch)
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                self.logger.info("Batch processing task cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error processing batch queue: {e}")
                await asyncio.sleep(1)

    async def _process_batch(self, batch: List[Dict]):
        """Process a batch of messages"""
        if not batch:
            return

        try:
            # Group batch items by trading mode and account
            grouped_batch = self._group_batch_items(batch)

            for group_key, group in grouped_batch.items():
                self.logger.debug(f"Processing batch group: {group_key}, size: {len(group)}")
                if not group:
                    continue

                # Get the first item to determine common context
                first_item = group[0]
                context = first_item["context"]
                trading_mode = context["trading_mode"]

                # Extract all messages
                all_msgs = []
                for item in group:
                    all_msgs.extend(item["msgs"])
                    self.logger.debug(f"Batch item has {len(item['msgs'])} messages, [{item.get('cid', 'N/A')}]")

                # Handle empty batches
                if not all_msgs:
                    continue

                # Create batch update message if possible
                batch_msg = self._create_batch_message(all_msgs, context)


                cid = str(uuid4())
                # Queue for processing
                if batch_msg:
                    await self.pending_broadcasts.put({
                        "msgs": [batch_msg],
                        "context": context,
                        "cid": cid
                    })
                else:
                    # No batch message created, queue individual messages
                    await self.pending_broadcasts.put({
                        "msgs": all_msgs,
                        "context": context,
                        "cid": cid
                    })

        except Exception as e:
            self.logger.error(f"Error processing message batch: {e}")

    def _create_batch_message(self, msgs: List, context: Dict) -> Optional[Any]:
        """
        Create a batch update message if possible.

        Args:
            msgs: List of messages to batch
            context: Transaction context

        Returns:
            batch_msg: Batch update message or None if not possible
        """
        # Extract spot and derivative orders and cancellations
        spot_orders = []
        derivative_orders = []
        spot_cancellations = []
        derivative_cancellations = []
        other_msgs = []

        for msg in msgs:
            msg_type = str(type(msg).__name__)

            if "CreateSpotLimitOrder" in msg_type:
                spot_orders.append(msg)
            elif "CreateDerivativeLimitOrder" in msg_type:
                derivative_orders.append(msg)
            elif "CancelSpotOrder" in msg_type:
                spot_cancellations.append(msg)
            elif "CancelDerivativeOrder" in msg_type:
                derivative_cancellations.append(msg)
            else:
                other_msgs.append(msg)

        # If we have any non-batchable messages, return None
        if other_msgs:
            return None

        # If we don't have any batchable messages, return None
        if not (spot_orders or derivative_orders or spot_cancellations or derivative_cancellations):
            return None

        # Determine sender based on context
        if context["trading_mode"] == "direct":
            sender = context["account"].account_address
        else:  # authz mode
            sender = context["authz_info"]["granter"]

        # Create batch update message
        return self.composer.msg_batch_update_orders(
            sender=sender,
            spot_orders_to_create=spot_orders,
            derivative_orders_to_create=derivative_orders,
            spot_orders_to_cancel=spot_cancellations,
            derivative_orders_to_cancel=derivative_cancellations
        )

    def _group_batch_items(self, batch: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Group batch items by trading mode and account.

        Args:
            batch: List of batch items

        Returns:
            grouped: Dictionary of grouped batch items
        """
        grouped = {}

        for item in batch:
            context = item["context"]
            trading_mode = context["trading_mode"]

            # Create a unique key based on trading mode and account
            if trading_mode == "direct":
                key = f"direct_{context['account'].account_address}"
            else:  # authz mode
                key = f"authz_{context['authz_info']['granter']}"

            if key not in grouped:
                grouped[key] = []

            grouped[key].append(item)

        return grouped

    def _should_batch_messages(self, msgs: List) -> bool:
        """
        Determine if messages should be batched.

        Args:
            msgs: List of messages

        Returns:
            should_batch: Whether messages should be batched
        """
        # Types that can be batched
        BATCHABLE_TYPES = [
            "MsgCreateSpotLimitOrder",
            "MsgCreateDerivativeLimitOrder",
            "MsgCancelSpotOrder",
            "MsgCancelDerivativeOrder",
        ]

        if not msgs:
            return False

        # Check if all messages are batchable
        return all(
            any(batchable_type in str(type(msg).__name__) for batchable_type in BATCHABLE_TYPES)
            for msg in msgs
        )

    async def setup_authz_grants(self, granter: str, grantee: str, message_types: List[str], duration_days: int = 30):
        """
        Setup authz grants for a granter-grantee pair.

        Args:
            granter: Granter account address
            grantee: Grantee account address
            message_types: List of message types to authorize
            duration_days: Grant duration in days

        Returns:
            results: Dictionary of grant results by message type
        """
        try:
            # Get granter account
            granter_account = None
            if hasattr(self.mediator, 'accounts') and self.mediator.accounts:
                granter_account = self.mediator.accounts.get(granter)

            if not granter_account:
                raise ValueError(f"Granter account {granter} not found")

            if not granter_account.can_sign_transaction():
                raise ValueError(f"Granter account {granter} cannot sign transactions (read-only mode)")

            # Create direct processor if not already initialized
            if not self.direct_processor:
                self.direct_processor = DirectTransactionProcessor(
                    self.logger,
                    self.client,
                    self.composer,
                    self.network,
                    self.retry_handler,
                    self.debug
                )

            results = {}

            # Process each grant message with sync and delay
            for i, msg_type in enumerate(message_types):
                try:
                    # Insert a delay between transactions (except for the first one)
                    if i > 0:
                        await asyncio.sleep(5)  # Wait 5 seconds between grants

                    # Sync account sequence before each transaction
                    await self.direct_processor.sync_account_sequence(granter_account)
                    self.logger.info(f"Processing grant {i+1}/{len(message_types)} for {msg_type} with sequence {granter_account.sequence}")

                    # Create grant message
                    grant_msg = self.composer.MsgGrantGeneric(
                        granter=granter,
                        grantee=grantee,
                        msg_type=msg_type,
                        expire_in=duration_days * 24 * 3600
                    )

                    # Direct broadcast (granter must sign this)
                    context = {
                        "trading_mode": "direct",
                        "account": granter_account,
                        "is_sync": True,
                        "retry_count": 0,
                        "max_retries": 3
                    }

                    # Process transaction
                    result = await self.direct_processor.process_transaction([grant_msg], context)
                    results[msg_type] = result

                    if result and result.get("success"):
                        self.logger.info(f"Successfully granted {msg_type} from {granter} to {grantee}")
                        # Wait a bit after success before proceeding
                        await asyncio.sleep(1)
                    else:
                        self.logger.error(f"Failed to grant {msg_type} from {granter} to {grantee}: {result.get('error') if result else 'Unknown error'}")

                except Exception as e:
                    self.logger.error(f"Error setting up grant for {msg_type}: {e}")
                    results[msg_type] = {"success": False, "error": str(e)}

            return results

        except Exception as e:
            self.logger.error(f"Error in setup_authz_grants: {e}")
            return {msg_type: {"success": False, "error": str(e)} for msg_type in message_types}

    async def verify_authz_grants(self, granter: str, grantee: str, message_types: List[str]) -> Dict:
        """
        Verify that required authz grants exist and are valid.

        Args:
            granter: Granter account address
            grantee: Grantee account address
            message_types: List of message types to check

        Returns:
            results: Dictionary with verification results
        """
        results = {
            "missing": [],
            "expired": [],
            "expiring_soon": [],
            "valid": []
        }

        # Current time for expiration checks
        now = datetime.now()
        expiring_soon_threshold = now + timedelta(days=7)  # 7-day warning

        for msg_type in message_types:
            try:
                # Fetch grants
                grants = await self.client.fetch_grants(
                    granter=granter,
                    grantee=grantee,
                    msg_type_url=msg_type
                )

                # Check if grant exists for this message type
                if not grants or not grants.get("grants"):
                    results["missing"].append(msg_type)
                    continue

                # Check expiration
                for grant in grants["grants"]:
                    if "expiration" in grant:
                        expiration = datetime.fromisoformat(grant["expiration"].replace('Z', '+00:00'))

                        if expiration <= now:
                            results["expired"].append(msg_type)
                        elif expiration <= expiring_soon_threshold:
                            results["expiring_soon"].append(msg_type)
                        else:
                            results["valid"].append(msg_type)
                    else:
                        # No expiration means it's valid
                        results["valid"].append(msg_type)

            except Exception as e:
                self.logger.error(f"Error verifying grant for {msg_type}: {e}")
                results["missing"].append(msg_type)

        return results

    async def terminate(self, msg: str, **kwargs):
        """Clean shutdown of the component"""
        self.logger.info(f"Terminating {self.name}: {msg}")
        self._active = False

        # Signal shutdown to main run loop
        self._shutdown_event.set()

        # Cancel all tasks
        await self._cleanup()

        # Signal termination to base class
        await super().terminate(msg, **kwargs)

    async def _cleanup(self):
        """Clean up background tasks"""
        tasks = [self._broadcast_task, self._refresh_task, self._batch_task]

        for task in tasks:
            if task and not task.done():
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    self.logger.info(f"Task {task.get_name() if hasattr(task, 'get_name') else 'unknown'} cancelled")

        # Drain queues
        while not self.pending_broadcasts.empty():
            try:
                pending_broadcast = self.pending_broadcasts.get_nowait()
                self.logger.warning(f"Pending broadcast: {pending_broadcast}")
            except asyncio.QueueEmpty:
                self.logger.warning(f"No pending broadcast")
            finally:
                self.pending_broadcasts.task_done()
                break



##############################
### Transaction Processors ###
##############################

class TransactionProcessor(ABC):
    """
    Base class for transaction processing modes.
    """
    TIMEOUT_BLOCK_HEIGHT_OFFSET = 500

    def __init__(self, logger, client: AsyncClient, composer: Composer, network: Network, retry_handler: RetryHandler, debug:bool):
        """Initialize with common dependencies"""
        self.logger = logger
        self.client = client
        self.composer = composer
        self.network = network
        self.retry_handler = retry_handler
        self.debug = debug

    @abstractmethod
    async def simulate(self, messages: List, context: Dict)->Tuple[bool, None|Dict[str,Any], Optional[Transaction]]:
        """
        Simulate transaction and return success status with response.

        Args:
        - messages: List of transaction messages
        - context: Context dict containing account information and other metadata

        Returns:
        - success: Whether simulation was successful
        - sim_response: Simulation response from chain
        - tx: Transaction object used for simulation
        """
        pass

    @abstractmethod
    async def prepare_transaction(self, messages: List, context: Dict)->Tuple[bytes, Dict]:
        """
        Prepare transaction bytes ready for broadcast.

        Args:
        - messages: List of transaction messages
        - context: Context dict containing account information and other metadata

        Returns:
        - tx_bytes: Raw transaction bytes ready for broadcast
        - gas_info: Dictionay with gas information (gas_wanted, gas_fee)
        """
        pass

    @abstractmethod
    async def _update_sequence(self, context: Dict) -> None:
        """
        Update sequence numbers after successful broadcast.

        Args:
            context: Transaction context with account info
        """
        pass

    async def broadcast(self, tx_bytes: bytes, is_sync: bool) -> Dict:
        """
        Broadcast prepared transaction bytes.

        Args:
            tx_bytes: Raw transaction bytes
            is_sync: Whether to use sync or async broadcast mode

        Returns:
            response: Broadcast response from chain
        """
        try:
            if is_sync:
                response = await self.client.broadcast_tx_sync_mode(tx_bytes)
            else:
                response = await self.client.broadcast_tx_async_mode(tx_bytes)

            return response

        except Exception as e:
            self.logger.error(f"Error during broadcast: {str(e)}")
            raise

    async def process_transaction(self, messages: List, context: Dict) -> None|Dict:
        """
        Process a transaction from simulation through broadcast with integrated retry handling.

        Args:
            messages: List of transaction messages
            context: Transaction context

        Returns:
            result: Transaction result with metadata
        """
        operation_name = f"{self.__class__.__name__}_{context.get('trading_mode', 'unknown')}"
        max_retries = context.get("max_retries", 3)

        async def operation():
            """Inner operation to be retried"""
            def update_timeout_height(result):
                """Extract and update timeout height from broadcast result"""
                context["timeout_height"] = int(result['raw_log'].split(" ")[2].split(',')[0]) + self.TIMEOUT_BLOCK_HEIGHT_OFFSET

            try:
                # Simulate transaction
                success, sim_response, tx = await self.simulate(messages, context)
                if not success or not sim_response:
                    return {
                        "success": False,
                        "error": "Simulation failed",
                        "simulation_passed": False
                    }

                # Store simulation results in context
                context['sim_response'] = sim_response
                context['tx'] = tx

                # Validate simulation response
                valid, error = self._validate_simulation(sim_response)
                self.logger.info(f"Simulation validation result: {valid}, {error}")
                if not valid:
                    return {
                        "success": False,
                        "error": error or "Simulation validation failed",
                        "simulation_passed": False
                    }

                # Broadcast transaction
                if not self.debug:
                    is_sync = context.get("is_sync", True)

                    MAX_RETRIES = 3
                    DELAYS = [0, 1, 2]  # Progressive delays: 0s, 1s, 2s

                    result = {'success':False}
                    for attempt in range(MAX_RETRIES + 1):  # 0, 1, 2, 3 (4 total attempts)

                        # Prepare transaction with proper gas
                        context["gas_buffer_multiplier"] = 3
                        tx_bytes, gas_info = await self.prepare_transaction(messages, context)
                        self.logger.debug(f"Transaction prepared with gas: {gas_info}")

                        response = await self.broadcast(tx_bytes, is_sync)
                        result = self._process_broadcast_response(response, gas_info)

                        # Block height timeout (code 30)
                        if result['code'] == 30:
                            try:
                                update_timeout_height(result)
                            except Exception as e:
                                self.logger.error(f"Failed to update timeout height from broadcast result: {e} result is: {result}")
                            if attempt < MAX_RETRIES:
                                self.logger.warning(f"Timeout block height, retry {attempt + 1}/{MAX_RETRIES}")
                                continue
                            else:
                                self.logger.warning(f"Block height timeout after {MAX_RETRIES} retries, giving up")
                                break

                        # Mempool full (code 20) - check if we should retry
                        if result['code'] == 20:
                            if attempt < MAX_RETRIES:
                                delay = DELAYS[min(attempt, len(DELAYS) - 1)]
                                self.logger.info(f"Mempool full, retry {attempt + 1}/{MAX_RETRIES} in {delay}s")
                                await asyncio.sleep(delay)
                                continue
                            else:
                                self.logger.warning(f"Mempool full after {MAX_RETRIES} retries, giving up")
                                break

                        # Update sequence numbers on success
                        if result["success"]:
                            await self._update_sequence(context)

                        return result

                else:
                    self.logger.warning(f"In debug mode, no real transaction broadcasting.")
                    return None

            except Exception as e:
                self.logger.error(f"Error in transaction operation: {str(e)}")
                retry_possible, updated_context = await self.handle_error(e, context)

                # Update context for retry handler
                for key, value in updated_context.items():
                    context[key] = value

                # If not retryable, propagate the error
                if not retry_possible:
                    raise

                # Retryable error, let retry handler handle it
                raise e

        # Use the retry handler to execute with retry
        try:
            result = await self.retry_handler.execute_with_retry(
                operation=operation,
                error_handler=self.handle_error,
                context={
                    "component": operation_name,
                    "messages": [type(msg).__name__ for msg in messages],
                    "retry_count": context.get("retry_count", 0)
                }
            )
            return result
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "simulation_passed": context.get("simulation_passed", False)
            }

    def _validate_simulation(self, sim_response: Dict) -> Tuple[bool, Optional[str]]:
        """
        Validate simulation response to check for issues.

        Args:
            sim_response: Simulation response from chain

        Returns:
            is_valid: Whether the simulation is valid
            error: Error message if simulation is invalid
        """
        # Basic validation
        if not sim_response:
            return False, "Empty simulation response"

        if "result" not in sim_response:
            return False, "Invalid simulation response format"

        # Check for specific error codes in message responses
        result = sim_response.get("result", {})
        msg_responses = result.get("msgResponses", [])

        for msg_response in msg_responses:
            # Check for error fields in response
            if "code" in msg_response and msg_response["code"] != 0:
                return False, f"Message error: {msg_response.get('message', 'Unknown error')}"

        return True, None

    def _process_broadcast_response(self, response: Dict, gas_info: Dict) -> Dict:
        """
        Process broadcast response into standardized format.

        Args:
            response: Broadcast response from chain
            gas_info: Gas information dictionary

        Returns:
            result: Standardized transaction result
        """
        result = {
            "success": False,
            "gas_info": gas_info,
            "simulation_passed": True,
            "code": 1
        }

        if not response:
            result["error"] = "Empty broadcast response"
            return result

        tx_response = response.get("txResponse", {})
        code = tx_response.get("code", 1)  # 0 = success, != 0 is error
        self.logger.debug(f"broadcast response: {response}")

        result['code'] = code
        result["tx_hash"] = tx_response.get("txhash")
        result["raw_log"] = tx_response.get("rawLog")
        if code == 0:
            result["success"] = True
            result["height"] = tx_response.get("height")
        else:
            result["error"] = tx_response.get("rawLog", "Unknown error")

        return result

    async def _base_simulate(self, *, messages: List, account: Account, context_info: Dict) -> Tuple[bool, Optional[Dict], Optional[Transaction]]:
        """Base simulation logic shared by direct and authz processors"""

        async def simulation_operation():
            # Create fresh transaction with current sequence on each retry
            tx = (
                Transaction()
                .with_messages(*messages)
                .with_sequence(account.sequence)
                .with_account_num(account.address.get_number())
                .with_chain_id(self.network.chain_id)
            )

            # Prepare signature for simulation
            sign_doc = tx.get_sign_doc(account.public_key)
            signature = account.private_key.sign(sign_doc.SerializeToString())
            tx_raw_bytes = tx.get_tx_data(signature, account.public_key)

            # Execute simulation
            sim_response = await self.client.simulate(tx_raw_bytes)
            return sim_response, tx

        try:
            # Execute with retry - transaction is created fresh on each attempt
            sim_response, tx = await self.retry_handler.execute_with_retry(
                operation=simulation_operation,
                error_handler=self.handle_error,
                context=context_info
            )

            return True, sim_response, tx

        except Exception as e:
            self.logger.error(f"Simulation error: {str(e)}")
            return False, None, None


    #async def _base_simulate(self, *, tx: Transaction, account: Account, context_info: Dict) -> Tuple[bool, Optional[Dict], Optional[Transaction#]]:
    #    """Base simulation logic shared by direct and authz processors"""
    #    return sim_response =
    #    try:
    #        # Prepare signature for simulation
    #        sign_doc = tx.get_sign_doc(account.public_key)
    #        signature = account.private_key.sign(sign_doc.SerializeToString())
    #        tx_raw_bytes = tx.get_tx_data(signature, account.public_key)
    #        # Execute simulation
    #        sim_response = await self.client.simulate(tx_raw_bytes)
    #        #sim_response = await self.retry_handler.execute_with_retry(
    #        #    operation=lambda: self.client.simulate(tx_raw_bytes),
    #        #    error_handler=self.handle_error,
    #        #    context=context_info
    #        #)

    #        return True, sim_response, tx

    #    except Exception as e:
    #        self.logger.error(f"Simulation error: {str(e)}")
    #        return False, None, None

    async def _base_prepare_transaction(self, tx: Transaction, account: Account,
                                       sim_response: Dict, context: Dict) -> Tuple[bytes, Dict]:
        """Base transaction preparation logic shared by processors"""
        # Calculate gas requirements
        used_gas = int(sim_response["gasInfo"]["gasUsed"])
        gas_buffer_multiplier = context.get("gas_buffer_multiplier", 3)
        timeout_height = context["timeout_height"]
        gas_limit = int(used_gas + GAS_FEE_BUFFER_AMOUNT * gas_buffer_multiplier)
        gas_fee = "{:.18f}".format((GAS_PRICE * gas_limit) / pow(10, 18)).rstrip("0")

        # Add fees and timeouts
        fee = [
            self.composer.coin(
                amount=GAS_PRICE * gas_limit,
                denom=self.network.fee_denom,
            )
        ]

        # Add fees and timeout height to transaction
        tx = (
            tx
            .with_gas(gas_limit)
            .with_fee(fee)
            # .with_timeout_height(timeout_height)
            .with_memo(context.get("memo", ""))
        )

        # Sign transaction
        sign_doc = tx.get_sign_doc(account.public_key)
        signature = account.private_key.sign(sign_doc.SerializeToString())
        tx_raw_bytes = tx.get_tx_data(signature, account.public_key)

        gas_info = {
            "gas_wanted": gas_limit,
            "gas_fee": gas_fee
        }

        return tx_raw_bytes, gas_info

    async def handle_error(self, error: Exception, context: Dict) -> Tuple[bool, Dict]:
        """
        Base error handling logic for all transaction processors.

        Args:
            error: The exception that occurred
            context: Transaction context including account info

        Returns:
            can_retry: Whether a retry is possible
            updated_context: Potentially modified context for retry
        """
        error_str = str(error)
        updated_context = dict(context)

        # Handle sequence mismatch errors
        if "account sequence mismatch" in error_str:
            return await self._handle_sequence_mismatch(error, error_str, updated_context)

        # Handle timeout errors
        elif "timeout" in error_str.lower() or "deadline" in error_str.lower():
            return await self._handle_timeout_error(error, error_str, updated_context)

        # Handle gas errors
        elif "gas" in error_str.lower():
            if "insufficient" in error_str.lower():
                return await self._handle_insufficient_gas(error, error_str, updated_context)
            else:
                # Increase gas limit by default multiplier and retry
                updated_context["gas_buffer_multiplier"] = context.get("gas_buffer_multiplier", 1) * 1.2
                self.logger.info(f"Increasing gas buffer for retry: {updated_context['gas_buffer_multiplier']}")
                return True, updated_context

        # Handle other errors
        return await self._handle_other_error(error, error_str, updated_context)

    @abstractmethod
    async def _handle_sequence_mismatch(self, error: Exception, error_str: str, context: Dict) -> Tuple[bool, Dict]:
        """Handle sequence mismatch errors"""
        pass

    @abstractmethod
    async def _handle_timeout_error(self, error: Exception, error_str: str, context: Dict) -> Tuple[bool, Dict]:
        """Handle timeout errors"""
        pass

    @abstractmethod
    async def _handle_insufficient_gas(self, error: Exception, error_str: str, context: Dict) -> Tuple[bool, Dict]:
        """Handle insufficient gas errors"""
        pass

    @abstractmethod
    async def _handle_other_error(self, error: Exception, error_str: str, context: Dict) -> Tuple[bool, Dict]:
        """Handle other types of errors"""
        pass

    async def sync_account_sequence(self, account: Account)->bool:
        """
        Synchronizes account sequence directly from blockchain data.

        Args:
            client: AsyncClient instance
            account: Account object to synchronize

        Returns:
            bool: True if synchronization was successful, False otherwise
        """
        try:
            old_sequence = account.sequence

            # Fetch account data directly from the blockchain
            account_data = await self.client.fetch_account(account.account_address)

            if account_data:
                new_sequence = account_data.base_account.sequence

                if old_sequence != new_sequence:
                    account.sequence = new_sequence
                    self.logger.info(f"Synchronized sequence for {account.account_address}: {old_sequence} -> {new_sequence}")
                else:
                    self.logger.debug(f"Sequence already in sync for {account.account_address}: {new_sequence}")

                return True

            else:
                self.logger.warning(f"Failed to fetch account data for {account.account_address}")
                return False

        except Exception as e:
            self.logger.error(f"Error synchronizing sequence for {account.account_address}: {e}")
            return False

class DirectTransactionProcessor(TransactionProcessor):
    """
    Transaction processor for direct mode (account directly signs transactions).
    """

    async def simulate(self, messages: List, context: Dict) -> Tuple[bool, None|Dict[str,Any], Optional[Transaction]]:
        """Simulate transaction with direct account signing"""
        account: Account = context["account"]

        if not account.can_sign_transaction():
            self.logger.error(f"Account {account.account_address} cannot sign transactions (read-only mode)")
            return False, None, None

        # Use base simulation with retry
        return await self._base_simulate(
            messages=messages,
            account=account,
            context_info={"account": account, "msgs_len": len(messages)}
        )


    async def prepare_transaction(self, messages: List, context: Dict) -> Tuple[bytes, Dict]:
        """Prepare transaction with direct account signing"""
        account: Account = context["account"]
        self.logger.debug(f"context: {context}")
        sim_response = context["sim_response"]
        tx: Transaction = context["tx"]
        return await self._base_prepare_transaction(tx, account, sim_response, context)

    async def _handle_sequence_mismatch(self, error: Exception, error_str: str, context: Dict) -> Tuple[bool, Dict]:
        """Handle sequence mismatch errors for direct transactions"""
        account: Account = context["account"]
        updated_context = dict(context)

        try:
            # Try to extract correct sequence
            correct_sequence = get_correct_sequence_number(error_str)
            self.logger.info(f"Correcting sequence number for {account.account_address}: {account.sequence} -> {correct_sequence}")

            # Update sequence in account and context
            account.sequence = correct_sequence
            updated_context["sequence_corrected"] = (True, correct_sequence)

            # Sequence corrected, retry possible
            return True, updated_context

        except Exception as e:
            self.logger.error(f"Failed to extract sequence from error: {e}")
            return False, updated_context

    async def _handle_timeout_error(self, error: Exception, error_str: str, context: Dict) -> Tuple[bool, Dict]:
        """Handle timeout errors for direct transactions"""
        account: Account = context["account"]
        self.logger.warning(f"Transaction timeout for {account.account_address}, will retry")
        context["timeout_height"] += self.TIMEOUT_BLOCK_HEIGHT_OFFSET
        # Timeout errors are retriable
        return True, context

    async def _handle_insufficient_gas(self, error: Exception, error_str: str, context: Dict) -> Tuple[bool, Dict]:
        """Handle insufficient gas errors for direct transactions"""
        account: Account = context["account"]
        self.logger.error(f"Insufficient gas for {account.account_address}")
        # Cannot retry with insufficient gas
        return False, context

    async def _handle_other_error(self, error: Exception, error_str: str, context: Dict) -> Tuple[bool, Dict]:
        """Handle other errors for direct transactions"""
        account: Account = context["account"]
        self.logger.error(f"Unhandled error for {account.account_address}: {error_str}")
        return False, context

    async def _update_sequence(self, context: Dict) -> None:
        """Update sequence number after successful broadcast"""
        account: Account = context["account"]
        account.sequence += 1
        self.logger.debug(f"Updated sequence for {account.account_address} to {account.sequence}")


class AuthzTransactionProcessor(TransactionProcessor):
    """
    Transaction processor for authz mode where a grantee executes transactions
    on behalf of a granter using the authz module.
    """

    def __init__(self, logger, client, composer, network, retry_handler, grantee_pool,debug:bool):
        """Initialize with dependencies including grantee pool"""
        super().__init__(logger, client, composer, network, retry_handler, debug)
        self.grantee_pool: GranteePool = grantee_pool

    async def simulate(self, messages: List, context: Dict) -> Tuple[bool, None|Dict[str,Any], Optional[Transaction]]:
        """Simulate transaction with authz wrapping"""
        granter_account: Account = context["granter_account"]
        authz_info = context["authz_info"]
        grantee_accounts = context["grantee_accounts"]
        signing_grantees = context.get("signing_grantees", [])

        if not signing_grantees:
            self.logger.error(f"No signing grantees available for {granter_account.account_address}")
            return False, None, None

        # Get next available grantee from the pool
        grantee_state = await self.grantee_pool.get_next_grantee(signing_grantees)
        if not grantee_state:
            self.logger.error(f"No available grantees for {granter_account.account_address}")
            return False, None, None

        # Store grantee state in context for later use
        context["grantee_state"] = grantee_state
        grantee_address = grantee_state.address
        grantee_account: Account = grantee_accounts.get(grantee_address)
        if not grantee_account:
            self.logger.error(f"Grantee account {grantee_address} not found")
            return False, None, None

        # Sync grantee sequence before simulation
        _ = await self.sync_account_sequence(grantee_account)
        grantee_state.sequence = grantee_account.sequence

        try:
            # Wrap original messages in MsgExec
            exec_msg = self.composer.MsgExec(
                grantee=grantee_address,
                msgs=messages
            )

            # Use base simulation with retry
            success, sim_response, tx = await self._base_simulate(
                messages=[exec_msg],
                account=grantee_account,
                context_info={
                    "granter_account": granter_account,
                    "grantee_state": grantee_state,
                    "grantee_account": grantee_account
                }
            )

            if not success or sim_response is None:
                self.logger.error(f"Simulation failed for authz tx from {grantee_address} on behalf of {granter_account.account_address}")
                # Update grantee state with error
                await self.grantee_pool.update_grantee_state(
                    grantee_address,
                    False,
                    error="Simulation failed"
                )
                return False, None, None

            # Unpack MsgExec response to get underlying responses
            sim_res_msgs = sim_response.get("result", {}).get("msgResponses", [])
            if sim_res_msgs and len(sim_res_msgs) > 0:
                # Store both raw and unpacked responses for validation
                context["raw_sim_response"] = sim_response

                try:
                    first_msg = messages[0]
                    unpacked_response = self.composer.unpack_msg_exec_response(
                        underlying_msg_type=first_msg.__class__.__name__,
                        msg_exec_response=sim_res_msgs[0]
                    )
                    context["unpacked_sim_response"] = unpacked_response
                    self.logger.debug(f"Successfully unpacked simulation response")
                except Exception as e:
                    self.logger.error(f"Error unpacking simulation response: {e}")
                    context["unpacked_sim_response"] = None

            # await self.grantee_pool.update_grantee_state(
            #     grantee_address,
            #     True,  # Successful operation
            #     new_sequence=grantee_state.sequence + 1  # Increment sequence
            # )

            return True, sim_response, tx

        except Exception as e:
            self.logger.error(f"Simulation error for authz tx from {grantee_address} on behalf of {granter_account.account_address}: {str(e)}")
            # Update grantee state with error
            await self.grantee_pool.update_grantee_state(
                grantee_address,
                False,
                error=str(e)
            )
            raise

    async def prepare_transaction(self, messages: List, context: Dict) -> Tuple[bytes, Dict]:
        """Prepare authz transaction with grantee signing"""
        self.logger.debug(f"context: {context}")
        granter_account: Account = context["granter_account"]
        grantee_state: GranteeState = context["grantee_state"]
        grantee_address = grantee_state.address
        grantee_account: Account = context["grantee_accounts"].get(grantee_address)
        sim_response = context["sim_response"]
        tx: Transaction = context["tx"]
        return await self._base_prepare_transaction(tx,grantee_account,sim_response,context)

    async def _handle_sequence_mismatch(self, error: Exception, error_str: str, context: Dict) -> Tuple[bool, Dict]:
        """Handle sequence mismatch errors for authz transactions"""
        granter_account = context["granter_account"]
        grantee_state = context.get("grantee_state")
        updated_context = dict(context)

        # No grantee state - cannot retry
        if not grantee_state:
            return False, updated_context

        grantee_address = grantee_state.address

        try:
            # Try to extract correct sequence
            correct_sequence = get_correct_sequence_number(error_str)
            self.logger.info(f"Correcting sequence for grantee {grantee_address}: {grantee_state.sequence} -> {correct_sequence}")

            # Update sequence in grantee state
            await self.grantee_pool.update_grantee_state(
                grantee_address,
                False,  # Not a successful operation
                new_sequence=correct_sequence,
                error="Sequence mismatch"
            )

            grantee_account = context["grantee_account"]
            grantee_account.sequence = correct_sequence
            self.logger.debug(f"Updated grantee account {grantee_address} sequence to {correct_sequence}")

            # Update context with new grantee state
            updated_context["sequence_corrected"] = (True, correct_sequence)

            # Sequence corrected, retry possible
            return True, updated_context

        except Exception as e:
            self.logger.error(f"Failed to extract sequence from error: {e}")
            return False, updated_context

    async def _handle_timeout_error(self, error: Exception, error_str: str, context: Dict) -> Tuple[bool, Dict]:
        """Handle timeout errors for authz transactions"""
        context['timeout_height'] += self.TIMEOUT_BLOCK_HEIGHT_OFFSET
        grantee_state = context.get("grantee_state")
        updated_context = dict(context)
        # No grantee state - cannot retry
        if not grantee_state:
            return False, updated_context

        grantee_address = grantee_state.address

        self.logger.warning(f"Transaction timeout for grantee {grantee_address}, will retry")

        # Update grantee state with error
        await self.grantee_pool.update_grantee_state(
            grantee_address,
            False,
            error=f"Timeout: {error_str}"
        )

        # Try with different grantee
        updated_context.pop("grantee_state", None)
        return True, updated_context

    async def _handle_insufficient_gas(self, error: Exception, error_str: str, context: Dict) -> Tuple[bool, Dict]:
        """Handle insufficient gas errors for authz transactions"""
        grantee_state = context.get("grantee_state")
        updated_context = dict(context)

        # No grantee state - cannot retry
        if not grantee_state:
            return False, updated_context

        grantee_address = grantee_state.address

        self.logger.error(f"Insufficient gas for grantee {grantee_address}")

        # Update grantee state with error
        await self.grantee_pool.update_grantee_state(
            grantee_address,
            False,
            error=f"Insufficient gas: {error_str}"
        )

        # Try with different grantee
        updated_context.pop("grantee_state", None)
        return True, updated_context

    async def _handle_other_error(self, error: Exception, error_str: str, context: Dict) -> Tuple[bool, Dict]:
        """Handle other errors for authz transactions"""
        grantee_state = context.get("grantee_state")
        updated_context = dict(context)

        # No grantee state - cannot retry
        if not grantee_state:
            return False, updated_context

        grantee_address = grantee_state.address

        # Check for authorization errors
        if "authorization" in error_str.lower() or "permission" in error_str.lower():
            self.logger.error(f"Authorization error for grantee {grantee_address}: {error_str}")
        else:
            self.logger.error(f"Unhandled error for grantee {grantee_address}: {error_str}")

        # Update grantee state with error
        await self.grantee_pool.update_grantee_state(
            grantee_address,
            False,
            error=f"Error: {error_str}"
        )

        # Try with different grantee
        updated_context.pop("grantee_state", None)
        return True, updated_context

    async def _update_sequence(self, context: Dict) -> None:
        """Update sequence number after successful broadcast"""
        grantee_state = context["grantee_state"]

        # Update grantee state in pool
        await self.grantee_pool.update_grantee_state(
            grantee_state.address,
            True,  # Successful operation
            grantee_state.sequence + 1  # Increment sequence
        )

        self.logger.debug(f"Updated sequence for grantee {grantee_state.address} to {grantee_state.sequence + 1}")
