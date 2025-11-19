from dataclasses import dataclass
from typing import Optional, Dict
from decimal import Decimal
from datetime import datetime
from injective_trader.utils.enums import Side, OrderStatus, MarketType

@dataclass
class Order:
    """Comprehensive order data structure"""
    # Core order properties
    market_id: str
    market_type: MarketType
    subaccount_id: str
    order_side: Side
    price: Decimal
    quantity: Decimal
    order_type: str = "LIMIT"  # Default to limit order
    post_only:bool = False

    # Order state
    order_hash: Optional[str] = None
    fillable: Optional[Decimal] = None
    filled: Optional[Decimal] = None
    status: Optional[OrderStatus] = None

    # Order details
    margin: Optional[Decimal] = None
    leverage: Optional[Decimal] = None
    trigger_price: Optional[Decimal] = None

    # Metadata
    fee_recipient: str = ""  # Default empty string
    cid: Optional[str] = None  # Client order ID
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # Reference fields for derivative orders
    position_delta: Optional[Dict] = None
    payout: Optional[Decimal] = None

    # Transaction data
    tx_hash: Optional[str] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None

    def to_message_dict(self) -> Dict:
        """Convert order to chain message format"""
        order_type = f"{self.order_side.name}_PO"  # e.g., "BUY_PO", "SELL_PO"

        order_info = {
            "subaccountId": self.subaccount_id,
            "feeRecipient": self.fee_recipient,
            "price": str(self.price),
            "quantity": str(self.quantity)
        }
        if self.cid:
            order_info["cid"] = self.cid

        msg = {
            "marketId": self.market_id,
            "orderInfo": order_info,
            "orderType": order_type,
            "triggerPrice": str(self.trigger_price) if self.trigger_price else "0"
        }

        if self.market_type == MarketType.DERIVATIVE and self.margin:
            msg["margin"] = str(self.margin)

        return msg
