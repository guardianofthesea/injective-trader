from dataclasses import dataclass
from typing import Dict
from injective_trader.utils.enums import MarketType, Event


@dataclass(order=True)
class Notification:
    event: Event
    data: Dict