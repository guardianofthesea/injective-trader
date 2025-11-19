import asyncio
from decimal import Decimal
from injective_trader.core.component import Component
from injective_trader.domain.account.position import LiquidatablePosition
from injective_trader.domain.market.market import Market
from injective_trader.utils.retry import get_retry_handler
from injective_trader.utils.enums import Event
from injective_trader.domain.message import Notification

from pyinjective.core.market import DerivativeMarket
from pyinjective.client.model.pagination import PaginationOption

class HelixLiquidator(Component):
    def __init__(self, logger, composer_base):
        super().__init__(logger, composer_base)
        self.name = "Liquidator"

    async def initialize(self, **kwargs):
        """
        Initialize liquidator with configuration
        """

        # Get chain-specific configuration and setup retry handler
        self.config = kwargs.get("config", {})
        retry_config = self.config.get("RetryConfig", {})
        self.retry_handler = get_retry_handler(
            self.logger,
            retry_config,
            "Liquidator"
        )

        self.reconnection_delay = self.config.get("ReconnectionDelay", 5)

        # Get token decimals and denom to symbol from mediator (if available)
        self.denom_decimals = self.mediator.denom_decimals if hasattr(self.mediator, 'denom_decimals') else {}
        self.denom_to_symbol = self.mediator.denom_to_symbol if hasattr(self.mediator, 'denom_to_symbol') else {}

        self.logger.info("Helix Liquidator initialized successfully")

    async def run(self, **kwarg):
        """
            Start requesting liquidatable position information
        """

        def _process_liquidatable_position(position:dict, market:Market)->LiquidatablePosition:
            #{
            #   "ticker":"INJ/USDT PERP",
            #   "marketId":"0x17ef48032cb24375ba7c2e39f384e56433bcab20cbee9a7357e4cba2eb00abe6",
            #   "subaccountId":"",
            #   "direction":"short",
            #   "quantity":"0.00966730135521481",
            #   "entryPrice":"15980281.340438795311756819",
            #   "margin":"75611.273514",
            #   "liquidationPrice":"23334925.188149",
            #   "markPrice":"39291123.99",
            #   "aggregateReduceOnlyQuantity":"0",
            #   "updatedAt":"1705525203015",
            #   "createdAt":"-62135596800000"
            #},
            sdk_market = market.market
            return LiquidatablePosition(
                strategy_name="Liquidator",
                ticker=position['ticker'],
                market_id=position['marketId'],
                subaccount_id=position['subaccountId'],
                is_long=position['direction']=='long',
                quantity=position['quantity'],
                aggregate_reduce_only_quantity=Decimal(position['aggregateReduceOnlyQuantity']),
                entry_price=sdk_market.price_from_chain_format(Decimal(position['entryPrice'])),
                margin=sdk_market.price_from_chain_format(position['margin']),
                liquidation_price=sdk_market.price_from_chain_format(position['liquidationPrice']),
                mark_price=sdk_market.price_to_chain_format(position['markPrice'])
            )

        while True:
            positions = await self.retry_handler.execute_with_retry(
                operation=self.get_liquidable_positions,
                context={"component":"liquidator"}
            )

            for position in positions:
                market_id = position['marketId']
                market = self.mediator.markets[market_id]
                liquidatable_position = _process_liquidatable_position(position, market)
                notification = Notification(
                    event=Event.LIQUIDATION_INFO,
                    data={"liquidatable_position": liquidatable_position}
                )
                self.logger.critical(f"liquidatable position: {liquidatable_position}")
                await self.mediator.notify(notification)
            if len(positions) < 100:
                await asyncio.sleep(0.2)
            else:
                await asyncio.sleep(0.1)


    async def get_liquidable_positions(self)->list[dict[str,str]]:
        """
            Requesting liquidatable positions from the helix indexer
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

        pagination = PaginationOption(skip=0, limit=100)
        positions = (await self.client.fetch_derivative_liquidable_positions(
            pagination=pagination,
        ))['positions']
        return positions

    async def receive(self, event: Event, data: dict):
        pass
