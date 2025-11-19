from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from decimal import Decimal

class Risk(ABC):
    def __init__(self, logger, config):
        self.logger = logger
        self.config = config