from abc import ABC, abstractmethod
from ...core.component import Component

class ChainListener(Component, ABC):
    """Chain Listener Interface - defines methods any chain listener must implement"""
    
    @abstractmethod
    async def request_snapshot(self, market_id: str):
        """Request market data snapshot"""
        pass


__all__ = ['ChainListener', 'HelixChainListener']