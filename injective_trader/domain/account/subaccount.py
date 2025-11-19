from pyinjective.core.token import Decimal
from typing import List, Dict

from injective_trader.domain.account.position import Position
from injective_trader.domain.account.order import Order

class SubAccount:
    def __init__(self, logger, subaccount_id: str):
        self.logger = logger
        self.subaccount_id: str = subaccount_id
        self.portfolio: Dict[str, Deposit] = {} # key: denom
        self.positions: Dict[str, Position] = {} # key: market_id

        self.open_bid_orders: Dict[str, Dict[str, Order]] = {} # key: market_id -> {order_hash: Order}
        self.open_ask_orders: Dict[str, Dict[str, Order]] = {}
        self.traded_bid_orders: Dict[str, Dict[str, Order]] = {}
        self.traded_ask_orders: Dict[str, Dict[str, Order]] = {}

        # self.position

        # What about base_balance and quote_balance?
        ### TODO: Could be identified in strategies

class Deposit:
    def __init__(self, denom: str, total_balance: Decimal, available_balance: Decimal):
        self.denom: str = denom
        self.total_balance: Decimal = total_balance
        self.available_balance: Decimal = available_balance

    def update(self, total_balance: Decimal, available_balance: Decimal):
        self.total_balance: Decimal = total_balance
        self.available_balance: Decimal = available_balance