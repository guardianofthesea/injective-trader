import asyncio
from decimal import Decimal
from injective_trader.core.component import Component
from injective_trader.domain.account.position import LiquidatablePosition
from injective_trader.domain.market.market import Market
from injective_trader.utils.retry import get_retry_handler
from injective_trader.utils.enums import Event, MarketType
from injective_trader.domain.message import Notification

from pyinjective.core.market import DerivativeMarket
#from pyinjective.client.model.pagination import PaginationOption

class HelixL3Streamer(Component):
    def __init__(self, logger, composer_base):
        super().__init__(logger, composer_base)
        self.name = "L3Streamer"

    async def initialize(self, **kwargs):
        """
        Initialize L3 orderbook streamer with configuration
        """

        # Get chain-specific configuration and setup retry handler
        self.config = kwargs.get("config", {})
        retry_config = self.config.get("RetryConfig", {})
        self.retry_handler = get_retry_handler(
            self.logger,
            retry_config,
            "L3Streamer"
        )

        self.reconnection_delay = self.config.get("ReconnectionDelay", 5)

        # Get token decimals and denom to symbol from mediator (if available)
        self.denom_decimals = self.mediator.denom_decimals if hasattr(self.mediator, 'denom_decimals') else {}
        self.denom_to_symbol = self.mediator.denom_to_symbol if hasattr(self.mediator, 'denom_to_symbol') else {}

        self.logger.info("Helix L3OB streamer initialized successfully")

    async def run(self, **kwarg):
        """
            Start requesting L3 order book information
        """

        self.logger.debug(f"l3 ob streamer config:: {self.config}")
        market_ids = self.config.get("MarketIds")
        if not isinstance(market_ids, list):
            self.logger.warning("MarketIds is missing or not a valid list. Defaulting to an empty list.")
            market_ids = []
        markets = [self.mediator.markets[market_id] for market_id in market_ids]
        self.logger.info(f"l3 ob market ids: {market_ids}")
        sleep_interval = float(self.config['SleepInterval'])
        while True:
            for market in markets:
                l3_orderbook = await self.retry_handler.execute_with_retry(
                    operation=self.get_l3_order_book,
                    context={"component":"l3_streamer"},
                    market=market
                )
                #for position in positions:
                normalized_l3_orderbook = self.normalize_orderbook(l3_orderbook, market)
                notification = Notification(
                    event=Event.EXTERNAL_INFO,
                    data=normalized_l3_orderbook
                )
                await self.mediator.notify(notification)
            await asyncio.sleep(sleep_interval)
            self.logger.debug(f"Sleep for {sleep_interval}")


    def normalize_orderbook(self, orderbook_data:dict, market:Market)->dict[str,list]:

        normalized_orderbook = {"market_id": market.market_id, "type": "l3", "asks": [], "bids": []}

        for side_key, target_list in [("Bids", normalized_orderbook["bids"]), ("Asks", normalized_orderbook["asks"])]:
            for item in orderbook_data.get(side_key, []):
                try:
                    if not all(k in item for k in ["price", "quantity"]):
                        self.logger.warning(f"L3 item missing 'price' or 'quantity': {item}. Skipping.")
                        continue

                    _market = market.market
                    normalized_item = {}
                    #item.copy()
                    normalized_item["price"] = _market.price_from_extended_chain_format(Decimal(item["price"]))
                    normalized_item["quantity"] = _market.quantity_from_extended_chain_format(Decimal(item["quantity"]))
                    normalized_item['subaccount_id']=item['subaccountId']
                    normalized_item['order_hash']=item['orderHash']
                    target_list.append(normalized_item)
                except KeyError as e: # Should be caught by `all(k in item for k in ...)`
                    self.logger.error(f"Missing key {e} in L3 orderbook item: {item}")
                except Exception as e:
                    self.logger.error(f"Error normalizing L3 {side_key[:-1].lower()} item {item}: {e}")
        return normalized_orderbook


    async def get_l3_order_book(self, market:Market)->dict[str,list[tuple[str,str,str,str]]]:
        """
            Requesting l3 order bookfrom the helix chain
        """
        # {
        #    "positions":[
        #       {
        #          "ticker":"INJ/USDT PERP",
        #          "marketId":"0x17ef48032cb24375ba7c2e39f384e56433bcab20cbee9a7357e4cba2eb00abe6",
        #          "subaccountId":"0x0a5d67f3616a9e7b53c301b508e9384c6321be47000000000000000000000000",
        #          "direction":"short",
        #          "quantity":"0.00966730135521481",
        #          "entryPrice":"15980281.340438795311756819",
        #          "margin":"75611.273514",
        #          "liquidationPrice":"23334925.188149",
        #          "markPrice":"39291123.99",
        #          "aggregateReduceOnlyQuantity":"0",
        #          "updatedAt":"1705525203015",
        #          "createdAt":"-62135596800000"
        #       },
        #       {
        #          "ticker":"INJ/USDT PERP",
        #          "marketId":"0x17ef48032cb24375ba7c2e39f384e56433bcab20cbee9a7357e4cba2eb00abe6",
        #          "subaccountId":"0x0c812012cf492aa422fb888e172fbd6e19df517b000000000000000000000000",
        #          "direction":"short",
        #          "quantity":"0.066327809378915175",
        #          "entryPrice":"16031762.538045163086357667",
        #          "margin":"520412.029703",
        #          "liquidationPrice":"23409630.791347",
        #          "markPrice":"39291123.99",
        #          "aggregateReduceOnlyQuantity":"0",
        #          "updatedAt":"1705525203015",
        #          "createdAt":"-62135596800000"
        #       }
        #    ]
        # }
        if market.market_type == MarketType.SPOT:
            return await self.client.fetch_l3_spot_orderbook(market_id=market.market_id)
        elif market.market_type == MarketType.DERIVATIVE:
            return await self.client.fetch_l3_derivative_orderbook(market_id=market.market_id)
        else:
            raise Exception("Unsupported market")

    async def receive(self, event: Event, data: dict):
        pass
