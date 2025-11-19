"""
Strategy module for injective-trader framework.

This module provides the base Strategy class and related components for
implementing trading strategies on Injective Protocol.
"""

from .strategy import Strategy, StrategyResult
from .handlers import (
    UpdateHandler,
    OrderbookHandler,
    OracleHandler,
    BalanceHandler,
    DepositHandler,
    PositionHandler,
    OrderHandler,
    TradeHandler,
    ExternalInfoHandler,
    LiquidationHandler,
)
from .performance import PerformanceMetrics, Alert
from .risk import Risk

__all__ = [
    # Core strategy classes
    "Strategy",
    "StrategyResult",
    
    # Handler classes
    "UpdateHandler",
    "OrderbookHandler", 
    "OracleHandler",
    "BalanceHandler",
    "DepositHandler",
    "PositionHandler",
    "OrderHandler",
    "TradeHandler",
    "ExternalInfoHandler",
    "LiquidationHandler",
    
    # Performance and risk management
    "PerformanceMetrics",
    "Alert",
    "Risk",
]