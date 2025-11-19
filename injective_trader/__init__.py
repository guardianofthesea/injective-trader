"""
Injective Trader Framework - A comprehensive SDK for algorithmic trading on Injective Protocol.

This package provides a flexible, component-based architecture for building trading bots,
market making strategies, and other algorithmic trading applications.
"""

__version__ = "0.6.2"
__author__ = "Runxue Yu, Peiyun Jin"
__email__ = "runxue@injectivelabs.org, peiyun@injectivelabs.org"

from injective_trader.client.trader_client import TraderClient

__all__ = ["TraderClient", "__version__"]

# # Core exports
# from .core import (
#     Component,
#     Manager,
#     Mediator,
#     State,
#     IdleState,
#     RunningState, 
#     TerminatedState,
#     TaskManager
# )

# # Manager exports
# from .managers import (
#     AccountManager,
#     MarketManager,
#     StrategyManager,
#     RiskManager
# )

# # Strategy exports
# from .strategy import Strategy, StrategyResult, PerformanceMetrics

# # Domain exports
# from .domain.account.account import Account
# from .domain.account.order import Order
# from .domain.account.position import Position
# from .domain.market.market import Market
# from .domain.message import Notification

# # Utility exports
# from .utils.enums import Event, MarketType, Side, OrderType, UpdateType
# from .utils.config import read_config, create_default_config, BotConfig

# # Client entry point
# from .client import TraderClient, TraderClientError

# __all__ = [
#     # Core
#     'Component', 'Manager', 'Mediator', 'State', 'IdleState',
#     'RunningState', 'TerminatedState', 'TaskManager',
#     # Managers  
#     'AccountManager', 'MarketManager', 'StrategyManager', 'RiskManager',
#     # Strategy
#     'Strategy', 'StrategyResult', 'PerformanceMetrics',
#     # Domain
#     'Account', 'Order', 'Position', 'Market', 'Notification',
#     # Utils
#     'Event', 'MarketType', 'Side', 'OrderType',
#     # Client
#     'TraderClient', 'TraderClientError',
#     # Config
#     'read_config', 'create_default_config', 'BotConfig',
#     # Enums
#     'Event', 'MarketType', 'Side', 'OrderType', 'UpdateType',
#     # Version
#     '__version__',
# ]