from abc import ABC
from dataclasses import dataclass
from decimal import Decimal
from typing import Tuple, Generic, Generator, TypeVar, Iterator
import logging

from injective_trader.domain.account.order import Order
from injective_trader.utils.enums import OrderType


@dataclass
class L2PriceLevel:
    price: Decimal
    quantity: Decimal


class L3Order:
    def __init__(
        self,
        order_hash: str,
        subaccount_id: str,
        price: Decimal,
        quantity: Decimal,
        is_buy: bool,
        margin: None | Decimal = None,
    ):
        self.order_hash = order_hash
        self.subaccount_id = subaccount_id
        self.price = price
        self.quantity = quantity
        self.margin = margin
        self.is_buy = is_buy
        self.next: None | L3Order = None
        self.prev: None | L3Order = None


class L3PriceLevel(L2PriceLevel):
    def __init__(self,*, logger:logging.Logger, price: Decimal, quantity:Decimal=Decimal(0)):
        super().__init__(price, quantity)
        self.head:L3Order|None = None
        self.tail:L3Order|None = None
        self._order_count = 0
        self.logger = logger

    def remove(self, order_to_remove: L3Order) -> None:
        if self.head is None:
            self.logger.error(f"Attempted to remove order {order_to_remove.order_hash} from empty price level {self.price}")
            return
            #raise Exception("Price level is empty")
        self.quantity -= order_to_remove.quantity
        self._order_count -= 1

        if order_to_remove.prev:
            order_to_remove.prev.next = order_to_remove.next
        else:  # Node was head
            self.head = order_to_remove.next

        if order_to_remove.next:
            order_to_remove.next.prev = order_to_remove.prev
        else:  # Node was tail
            self.tail = order_to_remove.prev

        # Clean up pointers on removed node (optional, good practice)
        order_to_remove.prev = None
        order_to_remove.next = None

        if self.quantity < Decimal(0):
            self.logger.warning(f"Price level {self.price} quantity became negative ({self.quantity}). Resettingto 0.")
            self.quantity = Decimal(0)

        if self.head is None: # If list became empty
            self.tail = None


    def insert(self, node: L3Order):
        self.quantity += node.quantity
        node.prev = None
        node.next = None
        self._order_count += 1

        if self.tail is None:  # List is empty
            self.head = node
            self.tail = node
        else:  # Add to tail (FIFO for orders at the same price level)
            self.tail.next = node
            node.prev = self.tail
            self.tail = node

    # Iterator Protocol - enables for loops using generators
    def __iter__(self) -> Generator[L3Order, None, None]:
        """Enable 'for order in price_level' syntax using generator"""
        current = self.head
        while current is not None:
            yield current
            current = current.next

    def __reversed__(self) -> Generator[L3Order, None, None]:
        """Enable 'for order in reversed(price_level)' syntax using generator"""
        current = self.tail
        while current is not None:
            yield current
            current = current.prev

    # Length Protocol - enables len()
    def __len__(self) -> int:
        """Return the number of orders in this price level"""
        return self._order_count

    # Boolean Protocol - enables if price_level:
    def __bool__(self) -> bool:
        """Return True if price level has orders"""
        return self.head is not None

PriceLevelType = TypeVar('PriceLevelType', bound=L2PriceLevel)

class Orderbook(ABC, Generic[PriceLevelType]):
    def __init__(self, *, logger: logging.Logger, sequence: int, is_healthy: bool) -> None:
        self.logger = logger
        self.sequence = sequence
        self.is_healthy: bool = is_healthy
        self.bids: list[PriceLevelType] = []
        self.asks: list[PriceLevelType] = []

    def tob(self) -> Tuple[None|Decimal, None|Decimal]: # Use Optional
        tob_bid = self.bids[0].price if self.bids else None
        tob_ask = self.asks[0].price if self.asks else None
        return tob_bid, tob_ask

    def validate_orderbook(self) -> bool:
        if not self.bids or not self.asks:
            if self.is_healthy:
                self.is_healthy = False
            # self.logger.debug("Orderbook unhealthy: bids or asks are empty.") # Can be noisy
            return False

        best_bid_price = self.bids[0].price
        best_ask_price = self.asks[0].price

        if best_bid_price >= best_ask_price:
            self.logger.warning(
                f"Invalid order book: best bid ({best_bid_price}) >= best ask ({best_ask_price})"
            )
            if self.is_healthy:
                self.is_healthy = False
            return False
        return True

    def clear(self):
        self.bids.clear()
        self.asks.clear()
        self.sequence = 0
        self.is_healthy = False

    def _get_side_list(self, is_buy: bool) -> list[PriceLevelType]:
        return self.bids if is_buy else self.asks

    # --- Sequence Handling Helpers (Primarily for L2 style updates) ---
    def _can_process_sequence(self, new_sequence: int, market_ticker: str = "", force_update: bool = False) -> bool:
        """Checks if a new sequence number can be processed."""
        self.logger.debug(f"Sequence check for {market_ticker}: Current: {self.sequence}, New: {new_sequence}, Force: {force_update}")

        if self.sequence == 0 and not force_update: # Initial snapshot expected
            self.logger.info(f"Initializing orderbook {market_ticker} with sequence {new_sequence}")
            return True

        if force_update:
            self.logger.info(f"Forcing reinitialization of orderbook {market_ticker} with sequence {new_sequence}")
            return True # Caller should handle clearing

        if new_sequence <= self.sequence:
            # Only warn if it's not a replay of the same sequence, which might be ok in some contexts
            if new_sequence < self.sequence:
                 self.logger.warning(f"Ignoring outdated sequence for {market_ticker}: got {new_sequence}, current is {self.sequence}")
            else: # new_sequence == self.sequence
                 self.logger.debug(f"Received same sequence for {market_ticker}: {new_sequence}. No update needed unless forced.")
            # Do not change health status for merely old/same sequence, as the book is still valid.
            return False

        if new_sequence > self.sequence + 1:
            gap_size = new_sequence - self.sequence - 1
            self.logger.warning(f"Sequence gap detected for {market_ticker}: missing {gap_size} updates. Current: {self.sequence}, New: {new_sequence}")
            if self.is_healthy:
                self.is_healthy = False # A gap makes the book unhealthy
            return False # Or, some strategies might allow processing with a gap by clearing first.

        return True # new_sequence == self.sequence + 1

    def _commit_update(self, new_sequence: int, market_ticker: str = ""):
        """Commits the update by setting sequence and validating."""
        self.logger.debug(f"Committing update for {market_ticker}: {self.sequence} -> {new_sequence}")
        self.sequence = new_sequence
        # If sequence processing was fine, health is now determined by book validity
        self.is_healthy = self.validate_orderbook()


class L2Orderbook(Orderbook):
    def __init__(self, *, logger:logging.Logger, sequence:int, is_healthy:bool) -> None:
        super().__init__(logger=logger, sequence=sequence, is_healthy=is_healthy)

    def batch_update_orderbook(self, updates: dict[str, list[dict[str, Decimal]]], sequence: int, market_ticker:str="", force_update:bool=False):
        """
        Update both sides of the L2 order book based on snapshot or delta.
        """
        if force_update:
            self.logger.info(f"Force updating {market_ticker}, clearing book.")
            self.clear() # Clears bids, asks, sequence, and sets is_healthy=False

        if not self._can_process_sequence(sequence, market_ticker, force_update):
            return # Sequence check failed or update not needed

        asks_data = updates.get('asks', []) # Use .get for safety
        bids_data = updates.get('bids', []) # Use .get for safety

        self._batch_update_side(updates=asks_data, is_buy=False)
        self._batch_update_side(updates=bids_data, is_buy=True)

        self._commit_update(sequence, market_ticker)

    def _batch_update_side(self, *, updates: list[dict[str, Decimal]], is_buy: bool):
        """
        Update one side of the L2 order book. This replaces levels or removes them.
        The list of price levels for the side is then re-sorted.
        """
        self.logger.debug(f"Batch updating {'bids' if is_buy else 'asks'} with {len(updates)} L2 updates.")
        target_list = self._get_side_list(is_buy)

        # Convert updates to a dictionary for efficient lookup: {price: new_quantity}
        update_map: dict[Decimal, Decimal] = {}
        for update_item in updates:
            price = Decimal(update_item["p"])
            quantity = Decimal(update_item["q"])

            # Validate updates
            if price <= Decimal(0):
                self.logger.error(f"Invalid L2 delta update: price {price} <= 0. Update: {update_item}")
                if self.is_healthy: self.is_healthy = False
                continue # Skip this invalid update part
            if quantity < Decimal(0):
                self.logger.error(f"Invalid L2 delta update: quantity {quantity} < 0. Update: {update_item}")
                if self.is_healthy: self.is_healthy = False
                continue # Skip this invalid update part

            update_map[price] = quantity

        new_price_levels: list[L2PriceLevel] = []

        # Iterate through the existing levels on this side of the book
        for existing_level in target_list:
            if existing_level.price in update_map:
                # This level is mentioned in the updates
                new_quantity = update_map.pop(existing_level.price) # Process and remove from map
                if new_quantity > Decimal(0):
                    # Update the quantity for this price level
                    new_price_levels.append(L2PriceLevel(price=existing_level.price, quantity=new_quantity))
                # If new_quantity is 0, this level is effectively removed (not added to new_price_levels)
            else:
                # This existing level was not in the updates, so it remains unchanged
                new_price_levels.append(existing_level) # Re-add the existing L2PriceLevel object

        # Add any new price levels from 'updates' that were not in 'current_levels_on_side'
        # These are the items remaining in 'update_map'
        for price, quantity in update_map.items():
            if quantity > Decimal(0):
                new_price_levels.append(L2PriceLevel(price=price, quantity=quantity))

        # Update the actual bids/asks list in the order book
        if is_buy:
            self.bids = sorted(new_price_levels, key=lambda x: x.price, reverse=True)
        else:
            self.asks = sorted(new_price_levels, key=lambda x: x.price)


@dataclass
class L3Orderbook(Orderbook):
    def __init__(self, *,logger:logging.Logger, sequence:int, is_healthy:bool) -> None:
        super().__init__(logger=logger, sequence=sequence, is_healthy=is_healthy)
        self.bid_order_map: dict[str, L3Order] = {}
        self.ask_order_map: dict[str, L3Order] = {}

    def _get_order_map(self, is_buy: bool) -> dict[str, L3Order]:
        return self.bid_order_map if is_buy else self.ask_order_map

    def _find_or_create_price_level(self, *,price: Decimal, is_buy: bool) -> L3PriceLevel:
        target_list:list[L3PriceLevel] = self._get_side_list(is_buy)
        # Find existing price level or determine insertion index
        for i, price_level in enumerate(target_list):
            if price_level.price == price:
                return price_level
            # Bids are sorted descending, Asks are ascending
            if (is_buy and price > price_level.price) or (not is_buy and price < price_level.price):
                new_pl = L3PriceLevel(logger=self.logger, price=price)
                target_list.insert(i, new_pl)
                return new_pl

        # Append if new price is at the end of the sorted list or list is empty
        new_pl = L3PriceLevel(logger=self.logger, price=price)
        target_list.append(new_pl)
        return new_pl

    def batch_update_orderbook(self,*,  updates: dict[str, list[dict[str, Decimal]]], sequence: int, market_ticker:str="", force_update:bool=True):
        asks_data = updates.get('asks', []) # Use .get for safety
        bids_data = updates.get('bids', []) # Use .get for safety
        if force_update:
            self.clear()

        self._batch_update_side(updates=asks_data, is_buy=False)
        self._batch_update_side(updates=bids_data, is_buy=True)

    def _batch_update_side(self, *,updates: list[dict], is_buy:bool):
        for order in updates:
            self.insert_order(
                order_hash=order['order_hash'],
                price=Decimal(order['price']),
                quantity=Decimal(order['quantity']),
                subaccount_id=order['subaccount_id'],
                is_buy=is_buy
            )

    def insert_order(
        self,
        *,
        order_hash: str,
        subaccount_id: str,
        price: Decimal,
        quantity: Decimal,
        is_buy: bool,
        margin: None|Decimal = None,
        order_type: OrderType = OrderType.LIMIT, # Make sure OrderType is defined
    ) -> None:
        if order_hash in self.bid_order_map or order_hash in self.ask_order_map:
            self.logger.warning(f"Order {order_hash} already exists. Use update_order or remove first.")
            return

        if price <= Decimal(0):
            self.logger.error(f"Invalid L3 insert: price {price} <= 0. Order: {order_hash}")
            return
        if quantity <= Decimal(0): # L3 orders usually must have positive quantity
            self.logger.error(f"Invalid L3 insert: quantity {quantity} <= 0. Order: {order_hash}")
            return

        if order_type == OrderType.LIMIT:
            order = L3Order(order_hash, subaccount_id, price, quantity, is_buy, margin)
            order_map = self._get_order_map(is_buy)

            price_level = self._find_or_create_price_level(price=price, is_buy=is_buy)
            price_level.insert(order)
            order_map[order_hash] = order
            # L3 typically doesn't have a global sequence number per order, but if it did:
            # self.is_healthy = self.validate_orderbook()
        else:
            self.logger.error(f"Order type {order_type} not implemented for L3 insert.")
            raise NotImplementedError(f"Order type {order_type} not implemented.")

    def remove_order(self, *, order_hash: str, is_buy: bool) -> None:
        order_map = self._get_order_map(is_buy)
        target_list:list[L3PriceLevel] = self._get_side_list(is_buy)

        if order_hash not in order_map:
            self.logger.warning(f"Order {order_hash} not found in {'bid' if is_buy else 'ask'} map for removal.")
            return

        order_to_remove = order_map.pop(order_hash)

        price_level_to_remove_idx = -1
        found_price_level = None

        for i, pl in enumerate(target_list):
            if pl.price == order_to_remove.price:
                found_price_level = pl
                pl.remove(order_to_remove)
                if pl.quantity == Decimal(0):
                    price_level_to_remove_idx = i
                break

        if found_price_level is None:
            self.logger.error(f"Order {order_hash} (price {order_to_remove.price}) was in map but its price level not found in book. Data inconsistency.")
            # This state is problematic. The order map and the book structure are out of sync.
            if self.is_healthy: self.is_healthy = False

        if price_level_to_remove_idx != -1:
            target_list.pop(price_level_to_remove_idx)
            self.logger.debug(f"Removed empty price level {order_to_remove.price} after removing order {order_hash}")

    def update_order(self, *, order_hash: str, new_quantity: None|Decimal = None, new_price: None|Decimal = None) -> None:
        """
        Updates an existing L3 order. This typically means changing its quantity.
        Changing price usually means removing the old order and inserting a new one.
        This implementation handles quantity update, or re-inserts if price changes.
        """
        order_to_update: None|L3Order = None
        is_buy: None|bool = None

        if order_hash in self.bid_order_map:
            order_to_update = self.bid_order_map[order_hash]
            is_buy = True
        elif order_hash in self.ask_order_map:
            order_to_update = self.ask_order_map[order_hash]
            is_buy = False
        else:
            self.logger.warning(f"Order {order_hash} not found for update.")
            return

        assert order_to_update is not None and is_buy is not None # For type checker

        old_price = order_to_update.price
        old_quantity = order_to_update.quantity

        # Determine new values
        final_price = new_price if new_price is not None else old_price
        final_quantity = new_quantity if new_quantity is not None else old_quantity

        if final_quantity <= Decimal(0):
            self.logger.info(f"Updating order {order_hash} to quantity {final_quantity}. Removing order.")
            self.remove_order(order_hash=order_hash, is_buy=is_buy)
            return

        if final_price <= Decimal(0):
            self.logger.error(f"Invalid L3 update: new price {final_price} <= 0 for order {order_hash}. Update rejected.")
            return

        # If price changes, it's a remove and insert
        if final_price != old_price:
            self.logger.debug(f"Price change for order {order_hash}. Re-inserting.")
            # Preserve other attributes
            subaccount_id = order_to_update.subaccount_id
            margin = order_to_update.margin

            self.remove_order(order_hash=order_hash, is_buy=is_buy)
            self.insert_order(
                order_hash=order_hash, # Re-using hash, ensure this is desired behavior
                subaccount_id=subaccount_id,
                price=final_price,
                quantity=final_quantity,
                is_buy=is_buy,
                margin=margin
                # order_type can be assumed LIMIT or retrieved if stored on L3Order
            )
        elif final_quantity != old_quantity: # Only quantity changed
            self.logger.debug(f"Quantity change for order {order_hash} at price {old_price}: {old_quantity} -> {final_quantity}")
            target_list:list[L3PriceLevel] = self._get_side_list(is_buy)
            found_pl = None
            for pl in target_list:
                if pl.price == old_price:
                    found_pl = pl
                    break

            if not found_pl:
                self.logger.error(f"Consistency error: Order {order_hash} found in map, but its price level {old_price} not in book side.")
                if self.is_healthy: self.is_healthy = False
                # Attempt to recover by re-inserting, or just log and mark unhealthy
                return

            quantity_diff = final_quantity - old_quantity
            found_pl.quantity += quantity_diff # Update total quantity at price level
            order_to_update.quantity = final_quantity # Update quantity on the order object

            if found_pl.quantity < Decimal(0): # Should not happen with valid updates
                self.logger.warning(f"Price level {found_pl.price} quantity became negative after update. Resetting to 0.")
                found_pl.quantity = Decimal(0)

            # Note: L3PriceLevel.remove/insert already updates quantity.
            # This direct manipulation is simpler if only quantity changes and order stays in place.
            current_health = self.is_healthy
            new_health = self.validate_orderbook()
            if current_health and not new_health:
                self.logger.warning(f"L3 book became invalid after updating order {order_hash}")
            self.is_healthy = new_health
        else:
            self.logger.debug(f"No change detected for order {order_hash}. Update skipped.")

    def valid_orderbook(self, operation:str):
        current_health = self.is_healthy
        new_health = self.validate_orderbook()
        if current_health and not new_health:
             self.logger.warning(f"L3 book became invalid after {operation}")
        self.is_healthy = new_health


    def clear(self):
        super().clear()
        self.bid_order_map.clear()
        self.ask_order_map.clear()
