import os
import grpc
import dotenv
import asyncio
import importlib
from typing import Dict
from decimal import Decimal

from pyinjective import AsyncClient
from pyinjective.core.network import Network, DisabledCookieAssistant
from pyinjective.wallet import PrivateKey
from pyinjective.core.market import SpotMarket, DerivativeMarket, BinaryOptionMarket

from injective_trader.utils.enums import MarketType, Side, Event
from injective_trader.utils.retry import get_retry_handler
from injective_trader.utils.oracle_type import PYTH_FEED_ID_TO_SYMBOL, STORK_ORACLE_SYMBOL, PROVIDER_ORACLE_SYMBOL
from injective_trader.agents.initializer import ExchangeInitializer
from injective_trader.domain.account.account import Account, BankBalance
from injective_trader.domain.account.subaccount import SubAccount, Deposit
from injective_trader.domain.account.position import Position
from injective_trader.domain.account.order import Order
from injective_trader.domain.market.market import Market
from injective_trader.domain.message import Notification
from injective_trader.managers.strategy_manager import StrategyManager
from injective_trader.managers.risk_manager import RiskManager
from injective_trader.strategy.strategy import Strategy

class HelixInitializer(ExchangeInitializer):
    def __init__(self, logger, config, mediator):
        super().__init__(logger, config, mediator)
        retry_config = config.get("RetryConfig", {})
        self.retry_handler = get_retry_handler(logger, retry_config, "HelixInitializer")

    #############################
    ### Client Initialization ###
    #############################

    async def initialize_client(self):
        '''
        Initialize Helix client
        ---
        OUTPUT
        - network
        - client
        - composer
        '''

        async def client_init_operation():
            network_name = self.config.get("Network", "mainnet")
            if network_name == "mainnet":
                self.network = Network.mainnet()
            elif network_name == "custom_secure":
                self.create_custom_network_secure_grpc()
            elif network_name == "custom_unsecure":
                self.create_custom_network_unsecure_grpc()
            else:
                self.network = Network.testnet()

            self.client = AsyncClient(network=self.network)
            await self.client.initialize_tokens_from_chain_denoms()
            await self.client.sync_timeout_height()
            return await self.client.composer()

        try:
            self.composer = await self.retry_handler.execute_with_retry(
                operation=client_init_operation,
                context={
                    "operation": "client_initialization",
                    "network": self.config["Network"]
                }
            )

            return {
                "network": self.network,
                "client": self.client,
                "composer": self.composer
            }
        except Exception as e:
            self.logger.error(f"Error setting up composer after retries: {repr(e)}")
            raise

    def create_custom_network_secure_grpc(self):
        network = Network.mainnet()
        network.chain_stream_endpoint = self.config["CustomNetworkEndpoint"].get('ChainStreamEndpoint', '127.0.0.1:1999')
        network.lcd_endpoint = self.config["CustomNetworkEndpoint"].get('LcdEndpoint', 'http://127.0.0.1:20337')
        network.grpc_endpoint = self.config["CustomNetworkEndpoint"].get('GrpcEndpoint', '127.0.0.1:1999')
        self.logger.critical(f"Secure custom grpc endpoint created  ")
        self.network = network

    def create_custom_network_unsecure_grpc(self):
        network = Network.mainnet()
        network.chain_cookie_assistant = DisabledCookieAssistant()
        network.grpc_channel_credentials = None
        network.chain_stream_channel_credentials = None

        # Rest of the endpoints
        network.chain_stream_endpoint = self.config["CustomNetworkEndpoint"].get('ChainStreamEndpoint', '127.0.0.1:1999')
        network.lcd_endpoint = self.config["CustomNetworkEndpoint"].get('LcdEndpoint', 'http://127.0.0.1:20337')
        network.tm_websocket_endpoint = self.config["CustomNetworkEndpoint"].get('TmWebSocketEndpoint', 'ws://127.0.0.1:36657/websocket')
        network.grpc_endpoint = self.config["CustomNetworkEndpoint"].get('GrpcEndpoint', '127.0.0.1:1999')
        self.logger.critical(f"GRPC endpoint: {network.grpc_endpoint}")
        self.network = network


    ##############################
    ### Account Initialization ###
    ##############################

    async def initialize_account(self):
        accounts_info_list = self._load_private_key()

        for account_info in accounts_info_list:
            # Create account based on whether it has credentials or not
            if account_info.get("has_credentials", False):
                account = Account(
                    self.logger,
                    account_info["bech32_address"],
                    address=account_info["address"],
                    private_key=account_info["private_key"],
                    public_key=account_info["public_key"]
                )
                # Only initialize sequence for accounts with credentials
                await account.address.async_init_num_seq(self.network.lcd_endpoint)
            else:
                account = Account(self.logger, account_info["bech32_address"])

            # Fetch balances and deposits for all accounts
            await self._fetch_balances_deposits(account)

            # Fetch positions and orders for all accounts
            for subaccount in account.subaccounts.values():
                self.logger.debug(f"fetching position for subaccount: {subaccount.subaccount_id}")
                await self._fetch_market_position(subaccount)
                self.logger.debug(f"fetched position for subaccount: {subaccount.subaccount_id}")
                for market in self.mediator.markets.values():
                    self.logger.debug(f"fetching open orders for market: {market.market_id} [{market.market.ticker}] {subaccount.subaccount_id}")
                    await self._fetch_open_orders(market, subaccount)
                    self.logger.debug(f"fetched open orders for market: {market.market_id} [{market.market.ticker}] {subaccount.subaccount_id}")
            await account.address.async_init_num_seq(self.network.lcd_endpoint)

            self.mediator.add_account(account)
            self.logger.debug(f"added account to mediator: {account_info['bech32_address']}")

    def _load_private_key(self):
        try:
            dotenv.load_dotenv()
            bot_name = self.config.get("BotName")

            # Get granter Private Key
            private_keys = {}
            granter_name = f"{bot_name}_GRANTER_INJECTIVE_PRIVATE_KEY"
            granter = os.getenv(granter_name)
            if granter:
                self.logger.info(f"Loaded private key for granter")
                private_keys["granter"] = granter
            else:
                raise Exception(f"Couldn't load private key for {granter_name}")

            # Get all grantee private keys by scanning environment variables
            import re
            grantee_pattern = re.compile(rf"^{re.escape(bot_name)}_GRANTEE_(\d+)_INJECTIVE_PRIVATE_KEY$")
            
            grantee_keys = {}
            for env_var, value in os.environ.items():
                match = grantee_pattern.match(env_var)
                if match:
                    grantee_index = int(match.group(1))
                    grantee_keys[grantee_index] = value
                    self.logger.info(f"Found private key for grantee {grantee_index}")
            
            # Sort grantee keys by index and add them to private_keys
            for grantee_index in sorted(grantee_keys.keys()):
                private_keys[f'grantee_{grantee_index}'] = grantee_keys[grantee_index]
                self.logger.info(f"Loaded private key for grantee {grantee_index}")
            
            if not grantee_keys:
                self.logger.warning(f"No grantee private keys found for bot {bot_name}")

            accounts_info_list = []

            # Process account addresses from config
            configured_addresses = self.config.get("AccountAddresses", [])

            # Process accounts with private keys if available
            if private_keys:
                self.logger.info("Private key(s) loaded...")

                for name, private_key_str in private_keys.items():
                    self.logger.info(f"loading {name}")
                    private_key = PrivateKey.from_hex(private_key_str)
                    public_key = private_key.to_public_key()
                    address = public_key.to_address()
                    bech32_address = public_key.to_address().to_acc_bech32()

                    accounts_info_list.append(
                        {
                            "private_key": private_key,
                            "public_key": public_key,
                            "address": address,  ## Address carries the number, sequence to be used in broadcast
                            "bech32_address": bech32_address,
                            "has_credentials": True
                        }
                    )

                    # Remove from configured addresses if present (to avoid duplicated)
                    if bech32_address in configured_addresses:
                        configured_addresses.remove(bech32_address)
            else:
                self.logger.warning("No private keys found. All accounts will be in read-only mode.")

            # Add remaining configured addresses as read-only accounts
            for bech32_address in configured_addresses:
                accounts_info_list.append(
                    {
                        "bech32_address": bech32_address,
                        "has_credentials": False
                    }
                )

            if not accounts_info_list:
                self.logger.warning("No accounts found. Please specify either private keys or account addresses.")

            return accounts_info_list
        except Exception as e:
            self.logger.error(f"Error loading account information: {e}")
            raise

    async def _fetch_balances_deposits(self, account: Account):
        '''
        NOTE: Fetch account balances and deposits using Indexer API
        '''
        async def fetch_operation(account_address: str):
            # Fetch all porfolios
            self.logger.debug(f"Executing fetch_operation for {account_address}")
            portfolio = await self.client.fetch_account_portfolio_balances(account_address)
            # self.logger.critical(f"Raw Portfolio Structure: {portfolio}")
            self.logger.debug(f"fetch_operation complete: go portfolio with keys {portfolio.keys() if portfolio else None}")
            return portfolio

        try:
            # Update bank balances
            self.logger.debug("Starting execute_with_retry for portfolio fetch")
            portfolio = await self.retry_handler.execute_with_retry(
                operation=fetch_operation,
                context={"operation": "fetch_balances", "account": account.account_address},
                account_address=account.account_address
            )
            self.logger.debug("execute_with_retry complete")

            for bank_balance in portfolio["portfolio"].get("bankBalances", []):
                denom = bank_balance["denom"]
                if denom not in self.mediator.denom_decimals:
                    symbol = self.mediator.denom_to_symbol.get(denom, "unknown token")
                    self.logger.warning(f"Found balance in token {symbol} ({denom}) which is not used in any configured market. "
                        f"This token's balance will be skipped in reporting. "
                        f"If you need to monitor this token, add a market that uses it to your configuration.")
                    continue

                amount = Decimal(bank_balance["amount"]) / self.mediator.denom_decimals[denom]
                if denom in account.bank_balances:
                    account.bank_balances[denom].update(amount)
                else:
                    account.bank_balances[denom] = BankBalance(denom, amount)

            # Update subaccount deposits
            for subaccount_deposit in portfolio["portfolio"].get("subaccounts", []):
                subaccount_id = subaccount_deposit["subaccountId"]
                if subaccount_id not in account.subaccounts:
                    account.subaccounts[subaccount_id] = SubAccount(self.logger, subaccount_id)

                denom = subaccount_deposit["denom"]
                if denom not in self.mediator.denom_decimals:
                    symbol = self.mediator.denom_to_symbol.get(denom, "unknown token")
                    self.logger.warning(f"Found balance in token {symbol} ({denom}) which is not used in any configured market. "
                        f"This token's balance will be skipped in reporting. "
                        f"If you need to monitor this token, add a market that uses it to your configuration.")
                    continue

                total = Decimal(subaccount_deposit["deposit"]["totalBalance"]) / self.mediator.denom_decimals[denom]
                available = Decimal(subaccount_deposit["deposit"]["availableBalance"]) / self.mediator.denom_decimals[denom]
                if denom in account.subaccounts[subaccount_id].portfolio:
                    account.subaccounts[subaccount_id].portfolio[denom].update(total, available)
                else:
                    account.subaccounts[subaccount_id].portfolio[denom] = Deposit(denom, total, available)

        except Exception as e:
            self.logger.error(f"Failed to fetch portfolio: {e}")
            raise

    # async def _fetch_balances_deposits(self, account: Account):
    #     """
    #     Fetch account balances and deposits using direct chain API
    #     """
    #     try:
    #         # Fetch bank balances
    #         async def fetch_bank_balances_operation(account_address: str):
    #             self.logger.debug(f"Fetching bank balances for {account_address}")
    #             return await self.client.fetch_bank_balances(address=account_address)

    #         bank_balances = await self.retry_handler.execute_with_retry(
    #             operation=fetch_bank_balances_operation,
    #             context={"operation": "fetch_bank_balances", "account": account.account_address},
    #             account_address=account.account_address
    #         )

    #         # Process bank balances
    #         for balance in bank_balances.get("balances", []):
    #             denom = balance["denom"]

    #             # Create denom_decimals entry if it doesn't exist
    #             if denom not in self.mediator.denom_decimals:
    #                 # For unknown tokens, use a safe default (usually 6 for most tokens)
    #                 self.mediator.denom_decimals[denom] = Decimal("1000000")  # Default to 6 decimals
    #                 self.logger.info(f"Added new token denom: {denom} with default 6 decimals")

    #             amount = Decimal(balance["amount"]) / self.mediator.denom_decimals[denom]
    #             if denom in account.bank_balances:
    #                 account.bank_balances[denom].update(amount)
    #             else:
    #                 account.bank_balances[denom] = BankBalance(denom, amount)

    #         # Step 2: Determine subaccounts and fetch deposits
    #         # We'll fetch deposits for 10 subaccounts (indices 0-9) by default
    #         # This is a reasonable limit for most users
    #         max_subaccount_index = 10

    #         for index in range(max_subaccount_index):
    #             subaccount_id = account.address.get_subaccount_id(index=index)

    #             # Skip if we've already processed this subaccount
    #             if subaccount_id in account.subaccounts:
    #                 continue

    #             # Fetch subaccount deposits
    #             async def fetch_subaccount_deposits_operation(subaccount_id: str):
    #                 self.logger.debug(f"Fetching deposits for subaccount {subaccount_id}")
    #                 return await self.client.fetch_subaccount_deposits(subaccount_id=subaccount_id)

    #             try:
    #                 deposits_response = await self.retry_handler.execute_with_retry(
    #                     operation=fetch_subaccount_deposits_operation,
    #                     context={"operation": "fetch_subaccount_deposits", "subaccount_id": subaccount_id},
    #                     subaccount_id=subaccount_id
    #                 )

    #                 # Check if the subaccount has any deposits
    #                 deposits = deposits_response.get("deposits", {})
    #                 if not deposits:
    #                     # Skip empty subaccounts to avoid clutter
    #                     continue

    #                 # Create subaccount if it doesn't exist
    #                 if subaccount_id not in account.subaccounts:
    #                     account.subaccounts[subaccount_id] = SubAccount(self.logger, subaccount_id)

    #                 # Process deposits
    #                 for denom, deposit_data in deposits.items():
    #                     # Skip if denom is not in denom_decimals and handle unknown tokens
    #                     if denom not in self.mediator.denom_decimals:
    #                         # For unknown tokens, use a safe default (usually 6 for most tokens)
    #                         self.mediator.denom_decimals[denom] = Decimal("1000000")  # Default to 6 decimals
    #                         self.logger.info(f"Added new token denom: {denom} with default 6 decimals")

    #                     # Parse deposit amounts
    #                     total = Decimal(deposit_data["totalBalance"]) / self.mediator.denom_decimals[denom]
    #                     available = Decimal(deposit_data["availableBalance"]) / self.mediator.denom_decimals[denom]

    #                     if denom in account.subaccounts[subaccount_id].portfolio:
    #                         account.subaccounts[subaccount_id].portfolio[denom].update(total, available)
    #                     else:
    #                         account.subaccounts[subaccount_id].portfolio[denom] = Deposit(denom, total, available)

    #             except Exception as e:
    #                 # Log but continue with other subaccounts if one fails
    #                 self.logger.warning(f"Failed to fetch deposits for subaccount {subaccount_id}: {e}")
    #                 continue

    #         # Log summary
    #         self.logger.info(f"Fetched balances for account {account.account_address}: "
    #                         f"{len(account.bank_balances)} bank balances, "
    #                         f"{len(account.subaccounts)} active subaccounts")

    #     except Exception as e:
    #         self.logger.error(f"Failed to fetch bank balances and deposits: {e}")
    #         raise

    async def _fetch_market_position(self, subaccount: SubAccount):
        '''
        Position for a specific market
        '''
        async def fetch_operation(subaccount_id: str):
            # Fetch initial positions for all subaccounts
            return await self.client.fetch_chain_subaccount_positions(subaccount_id)
            # position = await self.client.fetch_chain_subaccount_position_in_market(subaccount_id=subaccount.subaccount_id, market_id=market.market_id)

        try:
            position = await self.retry_handler.execute_with_retry(
                operation=fetch_operation,
                context={
                    "operation": "fetch_positions",
                    "subaccount_id": subaccount.subaccount_id
                },
                subaccount_id=subaccount.subaccount_id
            )

            for state in position["state"]:
                market_id = state["marketId"]
                if market_id not in self.mediator.markets:
                    market_ticker = self.mediator.id_to_ticker[market_id]
                    self.logger.warning(f"Position found in unmonitored market: {market_ticker} ({market_id}). "
                        f"This market is not in your configuration and will be ignored. "
                        f"To monitor this market, add it to the 'MarketTickers' section in your config.")
                    continue

                market: Market = self.mediator.markets[market_id]
                position = state["position"]

                # Process positions
                if market_id in subaccount.positions:
                    subaccount.positions[market_id].update(
                        quantity=market.market.quantity_from_extended_chain_format(Decimal(position["quantity"])),
                        entry_price=market.market.price_from_extended_chain_format(Decimal(position["entryPrice"])),
                        margin=market.market.margin_from_extended_chain_format(Decimal(position["margin"])),
                        cumulative_funding_entry=market.market.notional_from_extended_chain_format(Decimal(position["cumulativeFundingEntry"])),
                        is_long=position["isLong"],
                    )
                else:
                    subaccount.positions[market_id] = Position(
                        subaccount_id=subaccount.subaccount_id,
                        market_id=market_id,
                        quantity=market.market.quantity_from_extended_chain_format(Decimal(position["quantity"])),
                        entry_price=market.market.price_from_extended_chain_format(Decimal(position["entryPrice"])),
                        margin=market.market.margin_from_extended_chain_format(Decimal(position["margin"])),
                        cumulative_funding_entry=market.market.notional_from_extended_chain_format(Decimal(position["cumulativeFundingEntry"])),
                        is_long=position["isLong"],
                    )

        except Exception as e:
            self.logger.error(f"Failed to fetch positions and orders: {e}")
            raise

    async def _fetch_open_orders(self, market: Market, subaccount: SubAccount):
        # Fetch open orders
        async def fetch_operation(market_id: str, market_type: MarketType, subaccount_id: str):
            if market_type == MarketType.SPOT:
                return await self.client.fetch_chain_trader_spot_orders(market_id=market_id, subaccount_id=subaccount_id)
            elif market_type == MarketType.DERIVATIVE:
                return await self.client.fetch_chain_trader_derivative_orders(market_id=market_id, subaccount_id=subaccount_id)

            raise ValueError(f"Unsupported market type: {market_type}")

        try:
            orders = await self.retry_handler.execute_with_retry(
                operation=fetch_operation,
                context={
                    "operation": "fetch_orders",
                    "market_id": market.market_id,
                    "market_type": market.market_type,
                    "subaccount_id": subaccount.subaccount_id
                },
                market_id=market.market_id,
                market_type=market.market_type,
                subaccount_id=subaccount.subaccount_id,
            )

            if not orders or orders == {}:
                return


            if market.market_id not in subaccount.open_bid_orders:
                subaccount.open_bid_orders[market.market_id] = {}
            if market.market_id not in subaccount.open_ask_orders:
                subaccount.open_ask_orders[market.market_id] = {}
            if market.market_id not in subaccount.traded_bid_orders:
                subaccount.traded_bid_orders[market.market_id] = {}
            if market.market_id not in subaccount.traded_ask_orders:
                subaccount.traded_ask_orders[market.market_id] = {}

            for order in orders.get("orders", []):
                processed_order = Order(
                    order_hash=order["orderHash"],
                    subaccount_id=subaccount.subaccount_id,
                    market_id=market.market_id,
                    market_type=market.market_type,
                    order_side=Side.BUY if order["isBuy"] else Side.SELL,
                    price=market.market.price_from_extended_chain_format(Decimal(order["price"])),
                    quantity=market.market.quantity_from_extended_chain_format(Decimal(order["quantity"])),
                    fillable=market.market.quantity_from_extended_chain_format(Decimal(order["fillable"])),
                )

                if market.market_type == MarketType.DERIVATIVE:
                    processed_order.margin = market.market.margin_from_extended_chain_format(Decimal(order["margin"]))

                if order["isBuy"]:
                    subaccount.open_bid_orders[market.market_id][order["orderHash"]] = processed_order
                else:
                    subaccount.open_ask_orders[market.market_id][order["orderHash"]] = processed_order

        except Exception as e:
            self.logger.error(f"Failed to fetch orders from market {market.market_id}: {e}")
            raise


    #############################
    ### Market Initialization ###
    #############################

    async def initialize_markets(self):
        markets_dict = await self.setup_markets()

        def process_token(token, market_id, is_oracle_symbol=False):
            """
            Process a token to update various mapping dictionaries.

            Args:
                token: The token object with denom, symbol, and decimals attributes
                    or just a symbol string if is_oracle_symbol=True
                market_id: The ID of the market this token belongs to
                is_oracle_symbol: Boolean indicating if this is an oracle symbol (not a token symbol)
            """
            if not is_oracle_symbol:
                denom = token.denom
                symbol = token.symbol
                denom_decimals[denom] = Decimal(f"1e{token.decimals}")
                denom_to_symbol[denom] = symbol
                symbol_to_denom[symbol] = denom

                if symbol not in oracle_symbol_to_market:
                    oracle_symbol_to_market[symbol] = []
                oracle_symbol_to_market[symbol].append(market_id)

        # Build denom decimals and denom_to_symbol map
        denom_decimals = {}
        denom_to_symbol = {}
        symbol_to_denom = {}
        oracle_symbol_to_market = {}
        for market in markets_dict.values():
            market_id = market.id
            if isinstance(market, SpotMarket):
                # Process base and quote tokens
                process_token(market.base_token, market_id)
                process_token(market.quote_token, market_id)


            elif isinstance(market, DerivativeMarket):
                # Process quote token
                process_token(market.quote_token, market_id)

                # # Process oracle symbols
                # process_token(market.oracle_base, market_id, is_oracle_symbol=True)
                # process_token(market.oracle_quote, market_id, is_oracle_symbol=True)

        self.mediator.denom_decimals = denom_decimals
        self.mediator.denom_to_symbol = denom_to_symbol
        self.mediator.symbol_to_denom = symbol_to_denom

        # Process configured markets
        market_tickers = self.config["MarketTickers"]
        spot_market_ids = []
        derivative_market_ids = []

        for market_ticker in market_tickers:
            self.logger.debug(f"Work on market {market_ticker}")
            try:
                if market_ticker not in self.mediator.ticker_to_id:
                    raise Exception(f"Input market ticker {market_ticker} is not available on Helix.")
                market_id = self.mediator.ticker_to_id[market_ticker]
                market_data = markets_dict[market_id]

                if isinstance(market_data, DerivativeMarket):
                    original_base_symbol = market_data.oracle_base
                    self.logger.debug(f"oracle_type: {market_data.oracle_type}")
                    if market_data.oracle_type == "Pyth":
                        oracle_base = PYTH_FEED_ID_TO_SYMBOL.get(original_base_symbol, original_base_symbol).split('.')[-1].split('/')[0]
                    elif market_data.oracle_type == "Stork":
                        oracle_base = STORK_ORACLE_SYMBOL.get(original_base_symbol, original_base_symbol)
                    elif market_data.oracle_type == "Provider":
                        oracle_base = PROVIDER_ORACLE_SYMBOL.get(original_base_symbol, original_base_symbol)
                    else:
                        raise ValueError(f"Unknown oracle type: {market_data.oracle_type} in {market_ticker}")

                    if oracle_base not in oracle_symbol_to_market:
                        oracle_symbol_to_market[oracle_base] = []
                    oracle_symbol_to_market[oracle_base].append(market_id)

                    original_quote_symbol = market_data.oracle_quote
                    if market_data.oracle_type == "Pyth":
                        oracle_quote = PYTH_FEED_ID_TO_SYMBOL.get(original_quote_symbol, original_quote_symbol).split('.')[-1].split('/')[0]
                    elif market_data.oracle_type == "Stork":
                        oracle_quote = STORK_ORACLE_SYMBOL.get(original_quote_symbol, original_quote_symbol)
                    elif market_data.oracle_type == "Provider":
                        oracle_quote = PROVIDER_ORACLE_SYMBOL.get(original_quote_symbol, original_quote_symbol)
                    else:
                        raise ValueError(f"Unknown oracle type: {market_data.oracle_type}, {market_ticker}")

                    if oracle_quote not in oracle_symbol_to_market:
                        oracle_symbol_to_market[oracle_quote] = []
                    oracle_symbol_to_market[oracle_quote].append(market_id)

                    market = Market(self.logger, market_id, market_data, oracle_base, oracle_quote)

                else:
                    market = Market(self.logger, market_id, market_data)

                if isinstance(market_data, SpotMarket):
                    spot_market_ids.append(market_id)
                elif isinstance(market_data, DerivativeMarket):
                    derivative_market_ids.append(market_id)

                self.mediator.add_market(market)
                self.logger.info(f"Initialized market: {market_id} [{self.mediator.id_to_ticker[market_id]}]")

            except Exception as e:
                self.logger.error(f"Failed to initialize market {market_id}: {e}")
                raise

        self.mediator.oracle_symbol_to_market = oracle_symbol_to_market

        for spot_market_id in spot_market_ids:
            self.logger.info(f"Spot market: {spot_market_id} [{self.mediator.id_to_ticker[spot_market_id]}]")
        for derivative_market_id in derivative_market_ids:
            self.logger.info(f"Derivative market: {derivative_market_id} [{self.mediator.id_to_ticker[derivative_market_id]}]")

        if spot_market_ids:
            self.logger.info("Initializing spot orderbooks")
            await self._process_orderbooks(MarketType.SPOT, spot_market_ids)
        if derivative_market_ids:
            self.logger.info("Initializing derivative orderbooks")
            await self._process_orderbooks(MarketType.DERIVATIVE, derivative_market_ids)


    async def setup_markets(self):
        """
        Setup markets with retry for each market type
        """
        async def fetch_spot_markets():
            return await self.client.all_spot_markets()

        async def fetch_derivative_markets():
            return await self.client.all_derivative_markets()

        async def fetch_binary_markets():
            return await self.client.all_binary_option_markets()

        try:
            spot_markets = await self.retry_handler.execute_with_retry(
                operation=fetch_spot_markets,
                context={
                    "operation": "fetch_all_markets",
                    "market_type": "all_market_type"
                }
            )
            derivative_markets = self.client._derivative_markets
            binary_markets = self.client._binary_option_markets
            #derivative_markets = self.client._derivative_markets
            #derivative_markets = await self.retry_handler.execute_with_retry(
            #    operation=fetch_derivative_markets,
            #    context={
            #        "operation": "fetch_derivative_markets",
            #        "market_type": "derivative"
            #    }
            #)

            #binary_markets = await self.retry_handler.execute_with_retry(
            #    operation=fetch_binary_markets,
            #    context={
            #        "operation": "fetch_binary_markets",
            #        "market_type": "binary"
            #    }
            #)

            for market_id, market in spot_markets.items():
                self.mediator.ticker_to_id[market.ticker] = market_id
                self.mediator.id_to_ticker[market_id] = market.ticker

            for market_id, market in derivative_markets.items():
                self.mediator.ticker_to_id[market.ticker] = market_id
                self.mediator.id_to_ticker[market_id] = market.ticker

            return {
                **spot_markets,
                **derivative_markets,
                **binary_markets
            }

        except Exception as e:
            self.logger.error(f"Failed to setup markets: {e}")
            raise


    async def _process_orderbooks(self, market_type, market_ids):
        async def fetch_operation():
            if market_type == MarketType.SPOT:
                return await self.client.fetch_spot_orderbooks_v2(market_ids=market_ids)
            elif market_type == MarketType.DERIVATIVE:
                return await self.client.fetch_derivative_orderbooks_v2(market_ids=market_ids)
            else:
                raise Exception(f"Market type {market_type} is not implemented.")

        try:
            response = await self.retry_handler.execute_with_retry(
                operation=fetch_operation,
                context={"operation": "fetch_orderbooks"}
            )

            for orderbook_data in response["orderbooks"]:
                market_id = orderbook_data["marketId"]
                orderbook = orderbook_data["orderbook"]
                market: Market = self.mediator.markets[market_id]

                # Convert and validate sells
                sells = []
                for order in orderbook["sells"]:
                    price = market.market.price_from_chain_format(Decimal(order["price"]))
                    quantity = market.market.quantity_from_chain_format(Decimal(order["quantity"]))

                    # Validate price tick size
                    if price % market.min_price_tick == 0:
                        sells.append({"p": price, "q": quantity})
                    else:
                        self.logger.warning(f"Skipping ask with invalid tick size - price: {price}, tick: {market.min_price_tick}")

                # Convert and validate buys
                buys = []
                for order in orderbook["buys"]:
                    price = market.market.price_from_chain_format(Decimal(order["price"]))
                    quantity = market.market.quantity_from_chain_format(Decimal(order["quantity"]))

                    # Validate price tick size
                    if price % market.min_price_tick == 0:
                        buys.append({"p": price, "q": quantity})
                    else:
                        self.logger.warning(f"Skipping bid with invalid tick size - price: {price}, tick: {market.min_price_tick}")

                self.logger.info(f"Initializing orderbook: {market.market.ticker}")
                market.orderbook.batch_update_orderbook(updates={"asks":sells,"bids":buys}, sequence=int(orderbook["sequence"]), market_ticker=market.market.ticker, force_update=False)
                self.logger.info(f"Initialized orderbook: {market.market.ticker}")

        except Exception as e:
            self.logger.error(f"Failed to process orderbooks for {market_type}: {e}")
            raise


    ###############################
    ### Strategy Initialization ###
    ###############################

    async def initialize_strategies(self, strategy_manager: StrategyManager, strategies_config: Dict):
        try:
            for strategy_name, strategy_config in strategies_config.items():
                class_name = strategy_config["Class"]
                module = importlib.import_module("user." + class_name)
                strategy_class = getattr(module, class_name)
                self.logger.debug(f"Initialize strategy class {class_name} as {strategy_class}")
                self.logger.debug(f"Strategy config: {strategy_config}")

                strategy: Strategy = strategy_class(
                    logger=self.logger,
                    config=strategy_config,
                )

                # Filter accounts and markets specific to this strategy
                strategy_accounts = {
                    addr: account for addr, account in self.mediator.accounts.items()
                    if addr in strategy.account_addresses
                }

                strategy_markets = {
                    market_id: market for market_id, market in self.mediator.markets.items()
                    if market_id in strategy.market_ids
                }

                # Initialize the strategy with only relevant accounts and markets
                self.logger.debug("Initializing strategy")
                initialization_result = strategy.initialize(strategy_accounts, strategy_markets)
                self.logger.debug("Initialized strategy")

                # If strategy initialization returns orders to execute
                if initialization_result:
                    # Create notification for message broadcaster
                    notification = Notification(
                        event=Event.BROADCAST,
                        data={
                            "result": initialization_result,
                            "trading_mode": strategy.trading_mode,
                            "trading_info": {
                                "trading_account": strategy.trading_account if strategy.trading_mode == "direct" else     None,
                                "granter": strategy.granter if strategy.trading_mode == "authz" else None,
                                "grantees": strategy.grantees if strategy.trading_mode == "authz" else None
                            }
                        }
                    )
                    # Notify the mediator to handle the initialization orders
                    await self.mediator.notify(notification)
                    self.logger.info(f"Strategy {strategy_name} generated orders during initialization")
                else:
                    self.logger.debug("no strategy initialization result to send")

                strategy_manager.add_strategy(strategy)

        except Exception as e:
            self.logger.error(f"Error loading strategies: {e}")
            raise

    async def initialize_strategy(self, strategy_manager: StrategyManager, strategy_class, strategy_config: dict):
        try:
            self.logger.debug(f"Initialize strategy class {strategy_class.__name__} with config: {strategy_config}")
            strategy = strategy_class(
                logger=self.logger,
                config=strategy_config,
            )

            # Filter accounts and markets specific to this strategy
            strategy_accounts = {
                addr: account for addr, account in self.mediator.accounts.items()
                if addr in strategy.account_addresses
            }

            strategy_markets = {
                market_id: market for market_id, market in self.mediator.markets.items()
                if market_id in strategy.market_ids
            }

            # Initialize the strategy with only relevant accounts and markets
            self.logger.debug("Intializing strategy")
            initialization_result = strategy.initialize(strategy_accounts, strategy_markets)
            self.logger.debug("Intialized strategy")

            # If strategy initialization returns orders to execute
            if initialization_result:
                # Create notification for message broadcaster
                notification = Notification(
                    event=Event.BROADCAST,
                    data={
                        "result": initialization_result,
                        "trading_mode": strategy.trading_mode,
                        "trading_info": {
                            "trading_account": strategy.trading_account if strategy.trading_mode == "direct" else None,
                            "granter": strategy.granter if strategy.trading_mode == "authz" else None,
                            "grantees": strategy.grantees if strategy.trading_mode == "authz" else None
                        }
                    }
                )
                # Notify the mediator to handle the initialization orders
                await self.mediator.notify(notification)
                self.logger.info(f"Strategy {strategy.name} generated orders during initialization")
            else:
                self.logger.debug("no stategy initialization result to send")

            strategy_manager.add_strategy(strategy)

        except Exception as e:
            self.logger.error(f"Error loading strategies: {e}")
            raise

    async def initialize_risks(self, risk_manager: RiskManager, risks_config: Dict):
        try:
            module = importlib.import_module("risk")
            for risk_name, risk_config in risks_config.items():
                risk_class = getattr(module, risk_config["Class"])

                risk = risk_class(
                    logger=self.logger,
                    config=risk_config,
                )
                risk_manager.add_risk(risk)

        except Exception as e:
            self.logger.error(f"Error loading risks: {e}")
            raise
