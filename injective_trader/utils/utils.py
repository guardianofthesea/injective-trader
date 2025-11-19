from injective_trader.agents.initializer.helix_initializer import HelixInitializer
from injective_trader.utils.enums import Exchange

def get_initializer(exchange: str):
    initializers = {
        Exchange.HELIX.value.lower(): HelixInitializer,
    }

    exchange_lower = exchange.lower()

    if exchange_lower not in initializers:
        raise ValueError(f"Unsupported exchange: {exchange}")
    
    return initializers[exchange_lower]