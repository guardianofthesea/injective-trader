from typing import Dict
from decimal import Decimal
from datetime import datetime
from injective_trader.core.component import Manager
from injective_trader.domain.market.market import Market
from injective_trader.domain.message import Notification
from injective_trader.utils.enums import Event, UpdateType, MarketType

class MarketManager(Manager):
    def __init__(self, logger):
        super().__init__(logger)
        self.name = self.__class__.__name__
    #     self.markets: Dict[str, Market] = {}

    # def add_market(self, market: Market):
    #     self.markets[market.market_id] = market

    async def receive(self, event: Event, data: Dict):
        match event:
            case Event.ORDERBOOK_UPDATE:
                await self._update_orderbook(data)
            case Event.ORACLE_UPDATE:
                await self._update_oracle_price(data)

    async def _update_orderbook(self, data):
        '''
        Update orderbook for a specific market
        '''
        try:
            market: Market = data["market"]
            updates = data["updates"]
            sequence = data["sequence"]

            # Validate data
            if not self._validate_orderbook_data(market, updates, sequence):
                return

            self.logger.debug(f"Updating {market.market.ticker} orderbook {sequence}")
            market.orderbook.batch_update_orderbook(updates=updates, sequence=sequence, market_ticker=market.market.ticker)

            notification = Notification(
                event=Event.STRATEGY_UPDATE,
                data={
                    "update_type": UpdateType.OnOrderbook,
                    "market_id": market.market_id,
                    "market": market,
                    "sequence": sequence,
                    "block_time": data.get("block_time")
                }
            )
            await self.mediator.notify(notification)

        except Exception as e:
            self.logger.error(f"Error updating orderbook: {e}")
            raise

    def _validate_orderbook_data(self, market: Market, updates: Dict, sequence: int) -> bool:
        """Validate orderbook update data."""
        if not market:
            self.logger.error("Market object missing in update")
            return False

        if not updates or "bids" not in updates or "asks" not in updates:
            self.logger.error(f"Invalid orderbook update format for {market.market_id}")
            return False

        # Validate price levels against tick size
        for bid in updates["bids"]:
            if Decimal(str(bid["p"])) % market.min_price_tick != 0:
                self.logger.warning(f"Bid price {bid['p']} not aligned with tick size {market.min_price_tick}")
                return False

        for ask in updates["asks"]:
            if Decimal(str(ask["p"])) % market.min_price_tick != 0:
                self.logger.warning(f"Ask price {ask['p']} not aligned with tick size {market.min_price_tick}")
                return False

        return True

    async def _update_oracle_price(self, data: Dict):
        """
        Update oracle price for a specific market.

        data:
        - market_id: str
        - market: Market object
        - price: Decimal
        - symbol: str
        - type: str
        - block_height: int
        - timestamp: datetime
        """
        # oracle_price = OraclePrice(price=data["price"], symbol=data["symbol"], type=data["type"])
        market: Market = data["market"]
        symbol = data["symbol"]
        price = data["price"]
        oracle_type = data["type"]
        timestamp = data["timestamp"]

        market.update_oracle_price(symbol, price, oracle_type, timestamp)

        if market.oracle_price is not None:
            notification = Notification(
                event=Event.STRATEGY_UPDATE,
                data={
                        "update_type": UpdateType.OnOraclePrice,
                        "market_id": market.market_id,
                        "market": market,
                        "symbol": symbol,
                        "price": price,
                        "type": oracle_type,
                        "block_height": data.get("block_height")
                    }
            )
            await self.mediator.notify(notification)
