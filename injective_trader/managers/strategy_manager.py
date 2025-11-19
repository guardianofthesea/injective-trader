from typing import Dict, List, Set
from decimal import Decimal

from injective_trader.core.component import Manager
from injective_trader.domain.message import Notification
from injective_trader.utils.enums import Event, UpdateType
from injective_trader.utils.message_factory import MessageFactory
from injective_trader.strategy.strategy import Strategy, StrategyResult

class StrategyManager(Manager):
    def __init__(self, logger):
        super().__init__(logger)
        self.name = self.__class__.__name__
        self.strategies: Dict[str, Strategy] = {}
        self.market_strategy_map: Dict[str, Set[str]] = {}
        self.account_strategy_map: Dict[str, Set[str]] = {}
        # self.subaccount_strategy_map: Dict[str, Set[str]] = {}

    def add_strategy(self, strategy: Strategy):
        self.strategies[strategy.name] = strategy

        for market_id in strategy.market_ids:
            if market_id not in self.market_strategy_map:
                self.market_strategy_map[market_id] = set()
            self.market_strategy_map[market_id].add(strategy.name)

        for account_address in strategy.account_addresses:
            if account_address not in self.account_strategy_map:
                self.account_strategy_map[account_address] = set()
            self.account_strategy_map[account_address].add(strategy.name)

        # for subaccount_id in strategy.subaccount_ids:
        #     if subaccount_id not in self.subaccount_strategy_map:
        #         self.subaccount_strategy_map[subaccount_id] = set()
        #     self.subaccount_strategy_map[subaccount_id].add(strategy.name)

        #     account_address = self.mediator.subaccount_to_account(subaccount_id)
        #     if account_address not in self.account_strategy_map:
        #         self.account_strategy_map[account_address] = set()
        #     self.account_strategy_map[account_address].add(strategy.name)

    async def receive(self, event: Event, data: Dict):
        self.logger.debug(f"event: {event}")
        if event == Event.STRATEGY_UPDATE:
            strategies_to_execute = set()

            if "market_id" in data:
                market_id = data["market_id"]
                self.logger.debug(f"Calling strategies_to_execute.update with {market_id} {self.market_strategy_map.get(market_id, set())}")
                strategies_to_execute.update(self.market_strategy_map.get(market_id, set()))

            if "account_address" in data:
                account_id = data["account"]
                self.logger.debug(f"Calling strategies_to_execute.update with {account_id} {self.account_strategy_map.get(account_id, set())}")
                strategies_to_execute.update(self.account_strategy_map.get(account_id, set()))

            if "subaccount_id" in data:
                subaccount_id = data["subaccount_id"]
                account_address = self.mediator.subaccount_to_account[subaccount_id]
                self.logger.debug(f"Calling strategies_to_execute.update with {subaccount_id} {self.account_strategy_map.get(account_address, set())}")
                strategies_to_execute.update(self.account_strategy_map.get(account_address, set()))

            for strategy_name in strategies_to_execute:
                market_ids = self.strategies[strategy_name].market_ids
                account_addresses = self.strategies[strategy_name].account_addresses
                self.logger.debug(f"Sending notification to strategy:{strategy_name}")
                self.logger.debug(f"{data}")

                notification = Notification(
                    event = Event.DATA_REQUEST,
                    data={
                        "strategy_name": strategy_name,
                        "market_ids": market_ids,
                        "account_addresses": account_addresses,
                        "update_type": data["update_type"],
                        "update_data": {k: v for k, v in data.items() if k != "update_type"},
                    }
                )
                await self.mediator.notify(notification)

        elif event == Event.STRATEGY_EXECUTION:
            strategy_name = data["strategy_name"]
            strategy = self.strategies[strategy_name]

            self.logger.debug(f"Executing strategy:{strategy_name}")
            result = await strategy.execute(
                markets=data["markets"],
                accounts=data["accounts"],
                update_type=data["update_type"],
                update_data=data["update_data"],
            )

            if result:
                if strategy.risk:
                    notification = Notification(
                        event=Event.RISK_UPDATE,
                        data={
                            "strategy_name": strategy_name,
                            "risk_name": strategy.risk.name,
                            "markets": data["markets"],
                            "accounts": data["accounts"],
                            "result": result
                        }
                    )
                    await self.mediator.notify(notification)

                else:
                    if strategy.trading_mode:
                        notification = Notification(
                            event=Event.BROADCAST,
                            data={
                                "result": result,
                                "trading_mode": strategy.trading_mode,
                                "trading_info": {
                                    "trading_account": strategy.trading_account if strategy.trading_mode == "direct" else None,
                                    "granter": strategy.granter if strategy.trading_mode == "authz" else None,
                                    "grantees": strategy.grantees if strategy.trading_mode == "authz" else None
                                }
                            }
                        )
                        await self.mediator.notify(notification)

        else:
            raise Exception(f"Event {event} can not be handled in {self.__class__.__name__}")
