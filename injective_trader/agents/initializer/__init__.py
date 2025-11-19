from abc import ABC, abstractmethod
from typing import Dict, Any
from injective_trader.core.mediator import Mediator

class ClientInitializer(ABC):
    '''
    Base class for exchange-specific client initialization
    '''
    def __init__(self, logger, config, mediator):
        self.logger = logger
        self.config = config
        self.mediator: Mediator = mediator

    @abstractmethod
    async def load_credentials(self) -> Dict:
        '''
        Load private key or other credentials
        ---
        OUTPUT
        - accounts_info_list: list of {private_key, address, bech32_address}
        '''
        pass

    @abstractmethod
    async def setup_client(self) -> Dict:
        '''
        Initialize exchange client
        ---
        OUTPUT
        -
        '''
        pass

    @abstractmethod
    async def initialize(self) -> Dict:
        '''
        Aggregated initialization process
        ---
        OUTPUT
        - composer_base
        - markets_dict
        '''
        pass


class ExchangeInitializer(ABC):
    '''
    Base class for exchange-specific initialization
    '''
    def __init__(self, logger, config, mediator):
        self.logger = logger
        self.config = config
        self.mediator: Mediator = mediator

    @abstractmethod
    async def initialize_client(self)->Dict:
        '''
        Initialize exchange client and basic configurations
        '''
        pass

    @abstractmethod
    async def initialize_markets(self):
        '''
        Initialize orderbook and other market information
        '''
        pass

    @abstractmethod
    async def initialize_account(self):
        '''
        Initialize account balances, deposits, positions and open orders
        '''
        pass
