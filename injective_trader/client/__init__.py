"""
Client module for injective-trader framework.

This module provides the high-level client interface for users to interact
with the trading framework.
"""

from .trader_client import TraderClient, TraderClientError

__all__ = [
    "TraderClient",
    "TraderClientError", 
]