from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Optional

@dataclass
class LiquidatablePosition:
    strategy_name:str
    ticker:str
    market_id:str
    subaccount_id:str
    is_long:bool
    quantity:Decimal
    aggregate_reduce_only_quantity:Decimal
    entry_price:Decimal
    margin:Decimal
    liquidation_price:Decimal
    mark_price:Decimal

@dataclass
class Position:
    subaccount_id: str
    market_id: str
    quantity: Decimal
    entry_price: Decimal
    margin: Decimal
    cumulative_funding_entry: Decimal
    is_long: bool

    # PnL tracking
    unrealized_pnl: Decimal = Decimal("0")
    total_volume: Decimal = Decimal("0")
    trades_count: int = 0

    # Current market state
    mark_price: Optional[Decimal] = None
    liquidation_price: Optional[Decimal] = None
    margin_ratio: Optional[Decimal] = None

    def update(
        self,
        quantity: Optional[Decimal] = None,
        entry_price: Optional[Decimal] = None,
        margin: Optional[Decimal] = None,
        cumulative_funding_entry: Optional[Decimal] = None,
        is_long: Optional[bool] = None,
        mark_price: Optional[Decimal] = None,
        liquidation_price: Optional[Decimal] = None,
        margin_ratio: Optional[Decimal] = None
    ):
        """Update position fields and recalculate PnL if needed"""
        if quantity is not None:
            self.quantity = quantity
        if entry_price is not None:
            self.entry_price = entry_price
        if margin is not None:
            self.margin = margin
        if cumulative_funding_entry is not None:
            self.cumulative_funding_entry = cumulative_funding_entry
        if is_long is not None:
            self.is_long = is_long

        # Update market state
        if mark_price is not None:
            self._update_unrealized_pnl(mark_price)
        if liquidation_price is not None:
            self.liquidation_price = liquidation_price
        if margin_ratio is not None:
            self.margin_ratio = margin_ratio

    def update_from_trade(
        self,
        trade_price: Decimal,
        trade_quantity: Decimal,
        realized_pnl: Decimal,  ### TODO: where does this come from
        is_reduce_only: bool = False
    ):
        """Update position after trade execution"""
        # Update volume and trade count
        self.total_volume += trade_price * trade_quantity
        self.trades_count += 1

        # Recalculate unrealized PnL
        if self.mark_price:
            self._update_unrealized_pnl()

    def _update_unrealized_pnl(self, mark_price: Decimal | None):
        """Calculate unrealized PnL based on current mark price"""
        if mark_price is not None:
            self.mark_price = mark_price

        if not self.mark_price:
            self.unrealized_pnl = Decimal("0")
            return

        if self.quantity == 0:
            self.unrealized_pnl = Decimal("0")
            return

        price_diff = self.mark_price - self.entry_price
        if not self.is_long:
            price_diff = -price_diff

        self.unrealized_pnl = price_diff * self.quantity

    def to_dict(self) -> Dict:
        """Convert position to dictionary for reporting"""
        return {
            "market_id": self.market_id,
            "quantity": float(self.quantity),
            "entry_price": float(self.entry_price),
            "mark_price": float(self.mark_price) if self.mark_price else None,
            "is_long": self.is_long,
            "unrealized_pnl": float(self.unrealized_pnl),
            "total_volume": float(self.total_volume),
            "trades_count": self.trades_count,
            "margin_ratio": float(self.margin_ratio) if self.margin_ratio else None
        }
