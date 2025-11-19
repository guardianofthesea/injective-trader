from decimal import Decimal
from typing import Dict, Tuple, List, Any, TYPE_CHECKING
from abc import ABC, abstractmethod

from pyinjective.composer import Composer

from injective_trader.utils.enums import Side, MarketType
if TYPE_CHECKING:
    from injective_trader.domain.account.order import Order
from injective_trader.utils.enums import Side, MarketType
from injective_trader.strategy.strategy import StrategyResult

class MessageFactory(ABC):
    """Base class for exchange-specific message formatting"""

    @abstractmethod
    def create_messages(self, result: StrategyResult, broadcast_info: Dict) -> List[Any]:
        """Create all messages from strategy result"""
        pass

class HelixMessageFactory(MessageFactory):
    def __init__(self, logger, composer: Composer):
        self.logger = logger
        self.composer: Composer = composer

    def create_messages(self, result: StrategyResult | dict, broadcast_info: Dict) -> List[Any]:
        """Create all Injective chain messages from strategy result"""
        if not isinstance(result, StrategyResult):
            # Handle the case where result is a dict instead of StrategyResult
            if isinstance(result, dict):
                temp_result = StrategyResult()
                if "orders" in result:
                    temp_result.orders = result["orders"]
                if "cancellations" in result:
                    temp_result.cancellations = result["cancellations"]
                if "margin_updates" in result:
                    temp_result.margin_updates = result["margin_updates"]
                if "liquidations" in result:
                    temp_result.liquidations = result["liquidations"]
                result = temp_result
            else:
                raise ValueError(f"Expected StrategyResult but got {type(result)}")

        msgs = []
        trading_mode = broadcast_info["trading_mode"]
        trading_info = broadcast_info["trading_info"]

        # Get sender based on trading mode
        sender = (trading_info["trading_account"] if trading_mode == "direct"
                 else trading_info["granter"])

        # Handle individual orders (not suitable for batching)
        # Such as market orders that should be executed immediately
        for order in result.orders:
            if self._is_market_order(order):
                if order.market_type == MarketType.SPOT:
                    msg = self._create_spot_market_order(order, sender)
                elif order.market_type == MarketType.DERIVATIVE:
                    msg = self._create_derivative_market_order(order, sender)
                elif order.market_type == MarketType.BINARY:
                    self.logger.warning(f"Unsupported market type {order.market_type}")
                    continue
                else:
                    self.logger.warning(f"Unsupported market type {order.market_type}")
                    continue
                msgs.append(msg)

        # Filter out market orders that were already processed
        limit_orders = [o for o in result.orders if not self._is_market_order(o)]

        # Create batch update if there are limit orders or cancellations
        if limit_orders or result.cancellations:
            #spot_cancel_all = self._create_spot_cancel_all(result.cancellations)
            #derivative_cancel_all = self._create_derivative_cancel_all(result.cancellations)
            subaccount_id, derivative_cancel_all, spot_cancel_all =  self.create_cancel_all(result.cancellations)
            batch_msg = self.composer.msg_batch_update_orders(
                sender=sender,
                subaccount_id=subaccount_id,
                derivative_market_ids_to_cancel_all=derivative_cancel_all,
                spot_market_ids_to_cancel_all=spot_cancel_all,
                spot_orders_to_create=self._create_spot_orders(limit_orders),
                derivative_orders_to_create=self._create_derivative_orders(limit_orders),
                spot_orders_to_cancel=self._create_spot_cancellations(result.cancellations),
                derivative_orders_to_cancel=self._create_derivative_cancellations(result.cancellations)
            )
            msgs.append(batch_msg)

        # Handle margin updates
        for update in result.margin_updates:
            if update.get("action") == "increase":
                margin_msg = self._create_increase_margin_message(update, sender)
            else:
                margin_msg = self._create_decrease_margin_message(update, sender)
            msgs.append(margin_msg)

        # Handle liquidation positions
        for liquidation in result.liquidations:
            liquidation_msg = self._create_liquidation_message(liquidation, sender)
            msgs.append(liquidation_msg)

        return msgs

    def _is_market_order(self, order: "Order") -> bool:
        """Check if this is a market order (not suitable for batching)"""
        return order.order_type == "MARKET" if hasattr(order, "order_type") else False

    def _create_spot_market_order(self, order: "Order", sender: str) -> Dict:
        """Create spot market order message"""
        return self.composer.msg_create_spot_market_order(
            sender=sender,
            market_id=order.market_id,
            subaccount_id=order.subaccount_id,
            fee_recipient=order.fee_recipient,
            price=order.price,
            quantity=order.quantity,
            order_type=self._convert_order_type(order.order_side, order.post_only),
            cid=order.cid
        )

    def _create_derivative_market_order(self, order: "Order", sender: str) -> Dict:
        """Create derivative market order message"""
        if order.margin is None:
            raise ValueError(f"Margin is required for derivative orders: {order.order_hash}")
        self.logger.debug(f"Margin is set to: {order.margin}, for derivative orders: {order.order_hash}")
        return self.composer.msg_create_derivative_market_order(
            sender=sender,
            market_id=order.market_id,
            subaccount_id=order.subaccount_id,
            fee_recipient=order.fee_recipient,
            price=order.price,
            quantity=order.quantity,
            margin=order.margin,
            order_type=self._convert_order_type(order.order_side, order.post_only),
            cid=order.cid
        )

    def _create_spot_orders(self, orders: List["Order"]) -> List[Dict]|None:
        """Create spot limit order messages for batch update"""
        spot_orders = []
        for order in orders:
            if order.market_type == MarketType.SPOT:
                spot_order = self.composer.spot_order(
                    market_id=order.market_id,
                    subaccount_id=order.subaccount_id,
                    fee_recipient=order.fee_recipient,
                    price=order.price,
                    quantity=order.quantity,
                    order_type=self._convert_order_type(order.order_side, order.post_only),
                    cid=order.cid,
                    trigger_price=getattr(order, "trigger_price", None)
                )
                spot_orders.append(spot_order)
        return spot_orders if spot_orders else None

    def _create_derivative_orders(self, orders: List["Order"]) -> List[Dict]|None:
        """Create derivative limit order messages for batch update"""
        derivative_orders = []
        for order in orders:
            if order.market_type == MarketType.DERIVATIVE:
                if order.margin is None:
                    raise ValueError(f"Margin is required for derivative orders: {order.order_hash}")
                self.logger.debug(f"Margin is set to: {order.margin}, for derivative orders: {order.order_hash}")

                derivative_order = self.composer.derivative_order(
                    market_id=order.market_id,
                    subaccount_id=order.subaccount_id,
                    fee_recipient=order.fee_recipient,
                    price=order.price,
                    quantity=order.quantity,
                    margin=order.margin,
                    order_type=self._convert_order_type(order.order_side, order.post_only),
                    cid=order.cid,
                    trigger_price=getattr(order, "trigger_price", None)
                )
                derivative_orders.append(derivative_order)
        return derivative_orders if derivative_orders else None

    def create_cancel_all(self, cancellations: List[Dict]) -> Tuple[None|str, None|List[str], None|List[str]]:
        """
        Create spot and derivative cancellation messages for batch update.
        """
        # Initialize collections for different market types
        market_collections = {
            MarketType.SPOT: [],
            MarketType.DERIVATIVE: []
        }
        subaccount_ids = set()

        # Single-pass processing of all cancellations
        for cancellation in cancellations:
            market_type = cancellation.get("market_type")
            if market_type in market_collections:
                market_collections[market_type].extend(cancellation.get("market_ids", []))
                subaccount_ids.add(cancellation.get('subaccount_id', []))

        # Validate single subaccount constraint
        if len(subaccount_ids) > 1:
            self.logger.warning("can't use cancel all for more than one subaccount")
            return None, None, None

        # Extract final results
        subaccount_id = next(iter(subaccount_ids)) if subaccount_ids else None
        derivative_cancellations = market_collections[MarketType.DERIVATIVE] or None
        spot_cancellations = market_collections[MarketType.SPOT] or None

        return subaccount_id, derivative_cancellations, spot_cancellations

    def _create_spot_cancellations(self, cancellations: List[Dict]) -> List[Dict]|None:
        """Create spot cancellation messages for batch update"""
        spot_cancellations = []
        for cancellation in cancellations:
            if cancellation.get("market_type") != MarketType.DERIVATIVE:
                market_id = cancellation.get('market_id', None)
                if market_id is None:
                    continue
                # Create order data for cancellation
                order_data = self.composer.order_data(
                    market_id=cancellation["market_id"],
                    subaccount_id=cancellation["subaccount_id"],
                    order_hash=cancellation.get("order_hash"),
                    cid=cancellation.get("cid")
                )
                spot_cancellations.append(order_data)
        return spot_cancellations if spot_cancellations else None

    def _create_derivative_cancellations(self, cancellations: List[Dict]) -> List[Dict]|None:
        """Create derivative cancellation messages for batch update"""
        derivative_cancellations = []
        for cancellation in cancellations:
            if cancellation.get("market_type") == MarketType.DERIVATIVE:
                market_id = cancellation.get('market_id', None)
                if market_id is None:
                    continue
                # Create order data for cancellation
                order_data = self.composer.order_data(
                    market_id=cancellation["market_id"],
                    subaccount_id=cancellation["subaccount_id"],
                    order_hash=cancellation.get("order_hash"),
                    cid=cancellation.get("cid", None),
                    is_buy=cancellation.get("is_buy"),
                    is_conditional=cancellation.get("is_conditional"),
                    is_market_order=cancellation.get("is_market_order"),
                )
                derivative_cancellations.append(order_data)
        return derivative_cancellations if derivative_cancellations else None

    def _create_liquidation_message(self, liquidation: Dict, sender: str) -> Dict:
        """Create liquidation position message"""
        # Create order for liquidation

        liquidation_order = self.composer.derivative_order(
            market_id=liquidation["market_id"],
            subaccount_id=liquidation["executor_subaccount_id"],
            fee_recipient=liquidation.get("fee_recipient", ""),
            price=liquidation["price"],
            quantity=liquidation["quantity"],
            margin=liquidation["margin"],
            order_type=liquidation["order_type"],
            cid=liquidation.get("cid", "")
        )


        # Create liquidation message
        return self.composer.msg_liquidate_position(
            sender=sender,
            subaccount_id=liquidation["subaccount_id"],
            market_id=liquidation["market_id"],
            order=liquidation_order
        )

    def _create_increase_margin_message(self, update: Dict, sender: str) -> Dict:
        """Create margin increase message"""
        return self.composer.msg_increase_position_margin(
            sender=sender,
            market_id=update["market_id"],
            source_subaccount_id=update["source_subaccount_id"],
            destination_subaccount_id=update["destination_subaccount_id"],
            amount=update["amount"]
        )

    def _create_decrease_margin_message(self, update: Dict, sender: str) -> Dict:
        """Create margin decrease message"""
        return self.composer.msg_decrease_position_margin(
            sender=sender,
            market_id=update["market_id"],
            source_subaccount_id=update["source_subaccount_id"],
            destination_subaccount_id=update["destination_subaccount_id"],
            amount=update["amount"]
        )

    def _format_decimal_for_chain(self, value: Decimal, scale: int = 18) -> str:
        """Convert decimal to chain-compatible string format with proper scaling"""
        if not isinstance(value, Decimal):
            value = Decimal(str(value))

        # Scale the value appropriately for chain format
        scaled_value = int(value * Decimal(10**scale))
        return str(scaled_value)

    def _convert_order_type(self, side: Side, post_only:bool) -> str:
        """Convert internal Side enum to chain order type string"""
        if not post_only:
            if side == Side.BUY:
                return "BUY"
            elif side == Side.SELL:
                return "SELL"
        else:
            if side == Side.BUY:
                return "BUY_PO"
            elif side == Side.SELL:
                return "SELL_PO"
        raise ValueError(f"Unsupported order side: {side}")

class BinanceMessageFactory(MessageFactory):
    def create_messages(self, result: StrategyResult, broadcast_info: Dict) -> List[Any]:
        """Create all Binance API messages from strategy result"""
        # Similar structure but with Binance-specific message creation
        raise NotImplementedError
