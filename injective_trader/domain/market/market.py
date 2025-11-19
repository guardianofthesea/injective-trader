from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime
from pyinjective.core.token import Token
from pyinjective.core.market import BinaryOptionMarket, DerivativeMarket, SpotMarket

from injective_trader.domain.market.orderbook import L2Orderbook
from injective_trader.utils.enums import MarketType

class Market:
    def __init__(
            self,
            logger,
            market_id: str,
            market: BinaryOptionMarket | DerivativeMarket | SpotMarket,
            oracle_base: str | None = None,
            oracle_quote: str | None = None,
        ):
        self.logger = logger
        self.market_id: str = market_id
        self.market_ticker: str = market.ticker
        self.market_status: str = market.status
        self.market: BinaryOptionMarket | DerivativeMarket | SpotMarket = market

        # Set market type
        if isinstance(market, SpotMarket):
            self.market_type = MarketType.SPOT
        elif isinstance(market, DerivativeMarket):
            self.market_type = MarketType.DERIVATIVE
        elif isinstance(market, BinaryOptionMarket):
            self.market_type = MarketType.BINARY
        else:
            raise ValueError(f"Unknown market type for {self.market_ticker} ({self.market_id})")

        # Initialize orderbook
        self.orderbook = L2Orderbook(logger=logger, sequence=0, is_healthy=False)

        # Common token properties
        self.quote_token: Token = market.quote_token
        self.quote_denom = market.quote_token.denom
        self.quote_decimals = market.quote_token.decimals

        self.base_token: Token | None = None
        if self.market_type == MarketType.SPOT:
            self.base_token = market.base_token
            self.base_denom = market.base_token.denom
            self.base_decimals = market.base_token.decimals

        # Oracle properties
        self.oracle_base_symbol: str | None = None
        self.oracle_quote_symbol: str | None = None
        self.oracle_scale_factor: str | None = None
        self.oracle_type: str | None = None

        if self.market_type == MarketType.DERIVATIVE:
            self.oracle_base_symbol = oracle_base
            self.oracle_quote_symbol = oracle_quote
            self.oracle_scale_factor = market.oracle_scale_factor
            self.oracle_type = market.oracle_type

            # Add margin properties for derivatives
            self.initial_margin_ratio = market.initial_margin_ratio
            self.maintenance_margin_ratio = market.maintenance_margin_ratio

        # Fees
        self.maker_fee_rate: Decimal = self.market.maker_fee_rate
        self.taker_fee_rate: Decimal = self.market.taker_fee_rate
        self.service_provider_fee: Decimal = self.market.service_provider_fee

        # Oracle price tracking
        self.oracle_price: Decimal | None = None
        self.mark_price: Decimal | None = None
        self.base_oracle_price: Decimal | None = None # Price of base in USD
        self.quote_oracle_price: Decimal | None = None # Price of base in USD
        self.last_oracle_update_time: datetime | None = None
        self.last_oracle_price: Decimal | None = None

        # Convert tick sizes to human readable format
        if isinstance(market, (SpotMarket, DerivativeMarket)):
            self.min_price_tick = market.price_from_chain_format(
                market.min_price_tick_size
            )
            self.min_quantity_tick = market.quantity_from_chain_format(
                market.min_quantity_tick_size
            )

            self.min_notional_tick = market.notional_from_chain_format(
                market.min_notional
            )

    def update_oracle_price(self, symbol: str, price: Decimal, oracle_type: str, timestamp: datetime | None = None):
        ### FIXME: This function is exchange-specific for now
        """
        Update oracle price for a given symbol

        Args:
            symbol: Oracle symbol being updated
            price: New price
            timestamp: Optional timestamp of the update
        """
        now = timestamp or datetime.now()
        self.oracle_type = oracle_type
        raw_price = None

        # Update the appropriate oracle price based on symbol
        if self.market_type == MarketType.DERIVATIVE:
            if symbol == self.oracle_base_symbol:
                self.base_oracle_price = price
                self.logger.debug(f"Updated base oracle price ({symbol}): {price}")
            elif symbol == self.oracle_quote_symbol or symbol=="USDT":
                self.quote_oracle_price = price
                self.logger.debug(f"Updated quote oracle price ({symbol}): {price}")
            else:
                self.logger.warning(f"Symbol {symbol}, price {price} is neither the base oracle token:{self.oracle_base_symbol} nor the quote oracle token:{self.oracle_quote_symbol} of the derivative market {self.market_ticker}.")
        elif self.market_type == MarketType.SPOT:
            if symbol == self.base_token.symbol:
                self.base_oracle_price = price
            elif symbol == self.quote_token.symbol:
                self.quote_oracle_price = price
            else:
                self.logger.warning(f"Symbol {symbol}, price {price} is neither the base token:{self.oracle_base_symbol} nor the quote token:{self.oracle_quote_symbol} of the spot market {self.market_ticker}.")

        oracle_price = None

        # Calculate ratio if both prices are available
        if self.base_oracle_price is not None:
            if oracle_type == "provider":
                raw_price = self.base_oracle_price
            elif self.quote_oracle_price is not None and self.quote_oracle_price > 0:
                # Calculate the price ratio as base/quote
                raw_price = self.base_oracle_price / self.quote_oracle_price

        if raw_price:
            # Apply scaling factor if needed
            if self.oracle_scale_factor:
                oracle_price = raw_price # * (Decimal(f"1e{self.oracle_scale_factor}"))
                self.logger.debug(f"Applied scale factor {self.oracle_scale_factor} to price: {raw_price} -> {oracle_price}")
            else:
                oracle_price = raw_price

        # Check if this is significantly different from the last oracle price (more than 10%)
        if oracle_price is not None and self.oracle_price is not None:
            pct_diff = abs((oracle_price - self.oracle_price) / self.oracle_price * Decimal(100))
            if pct_diff > Decimal(10):
                self.logger.warning(
                    f"Large price discrepancy for {self.market_id}: now={oracle_price}, last={self.oracle_price} "
                    f"(diff: {pct_diff:.2f}%), "
                    f"with update time: now={now}, last={self.last_oracle_update_time}"
                )

        self.last_oracle_price = self.oracle_price
        self.oracle_price = oracle_price # // self.min_price_tick * self.min_price_tick if oracle_price else oracle_price

        if self.market_type == MarketType.DERIVATIVE:
            self.mark_price = self.oracle_price
            self.logger.debug(
                f"Updated {self.market_type} mark price for {self.market_ticker}: {self.oracle_price} "
                f"({self.base_oracle_price} / {self.quote_oracle_price})"
            )
        else:
            self.logger.debug(
                f"Updated {self.market_type} mark price for {self.market_ticker}: {self.oracle_price} "
                f"({self.base_oracle_price} / {self.quote_oracle_price})"
            )

        self.last_oracle_update_time = now

    def get_oracle_price(self) -> Decimal | None:
        """Get current oracle price if available"""
        return self.oracle_price

    def get_mark_price(self) -> Decimal:
        """Get mark price for the market"""
        if self.market_type == MarketType.DERIVATIVE:
            if self.mark_price:
                return self.mark_price
            elif self.oracle_price:
                return self.oracle_price
            else:
                self.logger.warning(
                    f"No mark price or oracle price available for market {self.market_ticker}, "
                    f"Using the mid price of TOB for now..."
                )
                bid, ask = self.orderbook.tob()
                if bid and ask:
                    return (bid + ask) / Decimal("2")
                else:
                    self.logger.warning(f"No mid price of TOB for {self.market_ticker}, return 0")
                    return Decimal("0")
        else:
            self.logger.warning(
                f"Market {self.market_ticker} doesn't have mark price since it is not a perpetual market."
                f"It is {self.market_type}. Oracle price is used here."
            )
            return self.oracle_price

    def is_oracle_fresh(self, max_age_seconds: int = 300) -> bool:
        """Check if oracle price is recent"""
        if not self.last_oracle_update_time:
            return False

        age = (datetime.now() - self.last_oracle_update_time).total_seconds()
        return age <= max_age_seconds

    def get_position_value(self, quantity: Decimal) -> Decimal:
        """Get position value based on current mark price"""
        return quantity * self.get_mark_price()

# @dataclass
# class OraclePrice:
#     price: Decimal
#     symbol: str
#     type: str
#     block_height: int
