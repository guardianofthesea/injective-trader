from typing import Dict
from decimal import Decimal
from injective_trader.core.component import Manager
from injective_trader.domain.account.account import Account, Balance, BankBalance
from injective_trader.domain.account.subaccount import SubAccount, Deposit
from injective_trader.domain.account.position import Position
from injective_trader.domain.account.order import Order
from injective_trader.domain.market.market import Market
from injective_trader.domain.message import Notification
from injective_trader.utils.enums import Event, Side, UpdateType, MarketType, OrderStatus

class AccountManager(Manager):
    def __init__(self, logger):
        super().__init__(logger)
        self.name = self.__class__.__name__
    #     self.accounts: Dict[str, Account] = {}

    # def add_account(self, account: Account):
    #     self.accounts[account.account_address] = account

    ###################
    ### Data Update ###
    ###################

    async def receive(self, event: Event, data: Dict):
        match event:
            case Event.BALANCE_UPDATE:
                await self._update_bank_balance(data)
            case Event.DEPOSIT_UPDATE:
                await self._update_deposit(data)
            case Event.POSITION_UPDATE:
                await self._update_position(data)
            case Event.SPOT_ORDER_UPDATE | Event.DERIVATIVE_ORDER_UPDATE:
                await self._update_order(event, data)
            case Event.SPOT_TRADE_UPDATE | Event.DERIVATIVE_TRADE_UPDATE:
                await self._update_trade(event, data)

    async def _update_bank_balance(self, data):
        self.logger.debug(f"Update bank balances: {data}")
        account: Account = data["account"]
        denom = data["denom"]
        amount = data["amount"]

        # Validate data
        if not self._validate_balance_update(account, denom, amount):
            return

        # Update balance state
        if denom in account.bank_balances:
            account.bank_balances[denom].update(data["amount"])
        else:
            account.bank_balances[denom] = BankBalance(denom, data["amount"])

        notification = Notification(
            event=Event.STRATEGY_UPDATE,
            data={
                "update_type": UpdateType.OnBankBalance,
                "account_address": account.account_address,
                "account": account,
                "balance": account.bank_balances[denom]
            }
        )
        await self.mediator.notify(notification)

    async def _update_deposit(self, data):
        self.logger.debug(f"Update deposit: {data}")
        account: Account = data["account"]
        subaccount: SubAccount = account.subaccounts[data["subaccount_id"]]
        denom = data["denom"]
        total = data["total"]
        available = data["available"]

        # Validate data
        if not self._validate_deposit_update(subaccount, denom, total, available):
            return

        # Update deposit state
        if denom in subaccount.portfolio:
            subaccount.portfolio[denom].update(data["total"], data["available"])
        else:
            subaccount.portfolio[denom] = Deposit(denom, data["total"], data["available"])

        notification = Notification(
            event=Event.STRATEGY_UPDATE,
            data={
                "update_type": UpdateType.OnDeposit,
                "account_address": account.account_address,
                "account": account,
                "subaccount_id": subaccount.subaccount_id,
                "deposit": subaccount.portfolio[denom]
            }
        )
        await self.mediator.notify(notification)

    async def _update_position(self, data):
        self.logger.debug(f"Update position: {data}")
        account: Account = data["account"]
        market_id = data["market_id"]
        quantity = data["quantity"]
        entry_price = data["entry_price"]

        subaccount_id = data["subaccount_id"]
        if subaccount_id not in account.subaccounts:
            account.subaccounts[subaccount_id] = SubAccount(self.logger, subaccount_id)

        subaccount: SubAccount = account.subaccounts[subaccount_id]

        # Validate data
        if not self._validate_position_update(subaccount, market_id, quantity, entry_price):
            return

        # Update Position
        if market_id in subaccount.positions:
            subaccount.positions[market_id].update(
                quantity=data["quantity"],
                entry_price=data["entry_price"],
                margin=data["margin"],
                cumulative_funding_entry=data["cumulative_funding_entry"],
                is_long=data["is_long"],
            )
        else:
            subaccount.positions[market_id] = Position(
                subaccount_id=subaccount_id,
                market_id=market_id,
                quantity=data["quantity"],
                entry_price=data["entry_price"],
                margin=data["margin"],
                cumulative_funding_entry=data["cumulative_funding_entry"],
                is_long=data["is_long"],
            )

        notification = Notification(
            event=Event.STRATEGY_UPDATE,
            data={
                "update_type": UpdateType.OnPosition,
                "account_address": account.account_address,
                "account": account,
                "subaccount_id": subaccount_id,
                "market_id": market_id,
                "position": subaccount.positions[market_id]
            }
        )
        await self.mediator.notify(notification)

    async def _update_order(self, event:Event,data: Dict):
        self.logger.debug(f"Update order: {data}")
        subaccount: SubAccount = data["account"].subaccounts[data["subaccount_id"]]
        market_id = data["market_id"]
        is_derivative = event == Event.DERIVATIVE_ORDER_UPDATE

        # Create order instance with updated fields
        order_data = {
            "market_id": market_id,
            "subaccount_id": data["subaccount_id"],
            "order_side": data["order_type"],
            "price": data["price"],
            "quantity": data["quantity"],
            "order_hash": data["order_hash"],
            "fillable": data["fillable"],
            "status": data["status"],
            "cid": data.get("cid"),
            "fee_recipient": data.get("fee_recipient", ""),
            ### TODO: create_at???
        }

        if is_derivative:
            order_data["market_type"] = MarketType.DERIVATIVE
            order_data["margin"] = data["margin"]
        else:
            order_data["market_type"] = MarketType.SPOT

        # Handle order cancellation
        if data["status"] == "Cancelled":
            order_store = (subaccount.open_bid_orders if data["order_type"] == Side.BUY
                  else subaccount.open_ask_orders)

            if market_id not in order_store:
                self.logger.warning(f"Received cancellation for market {market_id} but no orders tracked")
                return

            if data["order_hash"] not in order_store[market_id]:
                self.logger.warning(
                    f"Received cancellation for unknown order {data['order_hash']} in market {market_id} under subaccount {data['subaccount_id']}. Order may have been filled or removed already."
                )
                self.logger.debug(f"Current open orders: {list(order_store[market_id].keys())}")
                return

            order = order_store[market_id][data["order_hash"]]
            order_store[market_id].pop(data["order_hash"])

        # Handle order updates
        else:
            order = Order(**order_data)

            if data["order_type"] == Side.BUY:
                if market_id not in subaccount.open_bid_orders:
                    subaccount.open_bid_orders[market_id] = {}
                subaccount.open_bid_orders[market_id][data["order_hash"]] = order
            else:
                if market_id not in subaccount.open_ask_orders:
                    subaccount.open_ask_orders[market_id] = {}
                subaccount.open_ask_orders[market_id][data["order_hash"]] = order

        # Notify strategies
        update_type = UpdateType.OnDerivativeOrder if is_derivative else UpdateType.OnSpotOrder
        await self.mediator.notify(Notification(
            event=Event.STRATEGY_UPDATE,
            data={
                "update_type": update_type,
                "account": data["account"],
                "subaccount_id": data["subaccount_id"],
                "market_id": market_id,
                "order": order
            }
        ))

    async def _update_trade(self, event: Event, data: Dict):
        """Process trade update and notify strategies."""
        subaccount: SubAccount = data["account"].subaccounts[data["subaccount_id"]]
        market_id = data["market_id"]
        is_derivative = event == Event.DERIVATIVE_TRADE_UPDATE

        # Update open orders based on trade
        order_store = (subaccount.open_bid_orders if data["side"] == Side.BUY
                    else subaccount.open_ask_orders)
        trade_store = (subaccount.traded_bid_orders if data["side"] == Side.BUY
                    else subaccount.traded_ask_orders)

        try:
            # Check if we have the market tracked
            if market_id not in order_store:
                self.logger.warning(
                    f"Received trade for market {market_id} but no orders tracked for this market. "
                    f"Order hash: {data['order_hash']}, Side: {data['side']}"
                )
                return

            # Check if we have the specific order
            if data["order_hash"] not in order_store[market_id]:
                self.logger.warning(
                    f"Received trade for unknown order {data['order_hash']} in market {market_id}. "
                    f"This could indicate a state synchronization issue."
                )
                self.logger.debug(f"Current open orders for market: {list(order_store[market_id].keys())}")
                return

            # Ensure trade storage exists
            if market_id not in trade_store:
                trade_store[market_id] = {}

            open_order = order_store[market_id][data["order_hash"]]
            trade_quantity = (data["execution_quantity"] if is_derivative else data["quantity"])

            # Update fillable quantity
            open_order.fillable -= trade_quantity
            if open_order.fillable <= Decimal("0"):
                order_store[market_id].pop(data["order_hash"])

            # Create trade record
            trade_data = {
                "market_id": market_id,
                "subaccount_id": data["subaccount_id"],
                "order_side": data["side"],
                "order_hash": data["order_hash"],
                "price": data["execution_price"] if is_derivative else data["price"],
                "quantity": trade_quantity,
                "filled": trade_quantity,
                "status": OrderStatus.FILLED,
                "fee_recipient": open_order.fee_recipient,
                "cid": open_order.cid,
                ### TODO: create_at???
            }

            if is_derivative:
                trade_data["market_type"] = MarketType.DERIVATIVE
                trade_data["margin"] = data["execution_margin"]
                trade_data["position_delta"] = data.get("position_delta")
                trade_data["payout"] = data.get("payout")
            else:
                trade_data["market_type"] = MarketType.SPOT

            trade_order = Order(**trade_data)
            trade_store[market_id][data["order_hash"]] = trade_order

            # Notify strategies
            update_type = UpdateType.OnDerivativeTrade if is_derivative else UpdateType.OnSpotTrade
            await self.mediator.notify(Notification(
                event=Event.STRATEGY_UPDATE,
                data={
                    "update_type": update_type,
                    "account": data["account"],
                    "subaccount_id": data["subaccount_id"],
                    "market_id": market_id,
                    "trade": trade_order,
                    "order": open_order
                }
            ))

        except KeyError as e:
            self.logger.error(f"Error processing trade - order not found: {e}")
        except Exception as e:
            self.logger.error(f"Error processing trade: {e}")
            raise


    #######################
    ### Data Validation ###
    #######################

    def _validate_balance_update(self, account: Account, denom: str, amount: Decimal) -> bool:
        """Validate balance update data."""
        if not account:
            self.logger.error("Account object missing in update")
            return False

        if not denom:
            self.logger.error("Denom missing in balance update")
            return False

        if amount < 0:
            self.logger.error(f"Invalid negative amount in balance update: {amount}")
            return False

        return True

    def _validate_deposit_update(self, subaccount: SubAccount, denom: str,
                               total: Decimal, available: Decimal) -> bool:
        """Validate deposit update data."""
        if not subaccount:
            self.logger.error("Subaccount object missing in update")
            return False

        if not denom:
            self.logger.error("Denom missing in deposit update")
            return False

        if total < 0 or available < 0:
            self.logger.error(f"Invalid negative amounts in deposit update: total={total}, available={available}")
            return False

        if available > total:
            self.logger.error(f"Available amount ({available}) exceeds total ({total})")
            return False

        return True

    def _validate_position_update(self, subaccount: SubAccount, market_id: str,
                                quantity: Decimal, entry_price: Decimal) -> bool:
        """Validate position update data."""
        if not subaccount:
            self.logger.error("Subaccount object missing in update")
            return False

        if not market_id:
            self.logger.error("Market ID missing in position update")
            return False

        if entry_price <= 0:
            self.logger.error(f"Invalid entry price in position update: {entry_price}")
            return False

        return True
