from decimal import Decimal
from typing import Dict, Optional

from injective_trader.strategy.strategy import Strategy, StrategyResult
from injective_trader.domain.market.market import Market
from injective_trader.domain.account.order import Order
from injective_trader.domain.account.account import Account
from injective_trader.utils.enums import UpdateType, Side, MarketType

class SimpleStrategy(Strategy):
    """
    A simple market-making strategy that places buy orders at the current bid price
    and sell orders at the current ask price, respecting position limits.
    """
    
    def on_initialize(self, accounts: Dict[str, Account], markets: Dict[str, Market]) -> Optional[StrategyResult]:
        """
        Initialize strategy parameters and state.
        
        Args:
            accounts: Dictionary of account_address -> Account objects
            markets: Dictionary of market_id -> Market objects
            
        Returns:
            Optional initial orders as StrategyResult or None
        """
        self.logger.info(f"Initializing {self.name} strategy")
        
        # Load parameters from config
        params = self.config.get("Parameters", {})
        self.order_size = Decimal(str(params.get("OrderSize", "0.1")))
        self.max_position = Decimal(str(params.get("MaxPosition", "1.0")))
        self.spread_threshold = Decimal(str(params.get("SpreadThreshold", "0.005")))  # 0.5%
        
        # Initialize state
        self.last_prices = {}  # market_id -> last price
        self.active_orders = {}  # order_hash -> order details
        
        self.logger.info(f"Strategy initialized with parameters: OrderSize={self.order_size}, "
                        f"MaxPosition={self.max_position}, SpreadThreshold={self.spread_threshold}")
        
        # Return None (no initial orders)
        return None
    
    async def _execute_strategy(self, update_type: UpdateType, processed_data: Dict) -> Optional[StrategyResult]:
        """
        Core strategy logic responding to market events.
        
        Args:
            update_type: Type of update (UpdateType enum)
            processed_data: Update-specific data dictionary
            
        Returns:
            StrategyResult with orders or None
        """
        # Only process orderbook updates
        if update_type != UpdateType.OnOrderbook:
            return None
            
        market_id = processed_data.get("market_id")
        if not market_id or market_id not in self.markets:
            return None
            
        market = self.markets[market_id]
        
        # Get current market state
        bid, ask = market.orderbook.tob()
        if not bid or not ask:
            self.logger.warning(f"Incomplete orderbook for {market_id}: bid={bid}, ask={ask}")
            return None
            
        # Calculate spread
        spread_pct = (ask - bid) / bid
        if spread_pct > self.spread_threshold:
            self.logger.info(f"Spread {spread_pct:.2%} exceeds threshold {self.spread_threshold:.2%}")
            return None
            
        # Check for existing position
        subaccount_id = self.config.get("SubaccountIds", [""])[0]
        if not subaccount_id:
            # Try to get from config - different configs might have different structures
            for account_address in self.account_addresses:
                if account_address in self.accounts:
                    account = self.accounts[account_address]
                    # Get first available subaccount
                    if account.subaccounts:
                        subaccount_id = next(iter(account.subaccounts.keys()))
                        break
        
        if not subaccount_id:
            # self.logger.error("No valid subaccount found for order placement")
            # return None
            subaccount_id = account.address.get_subaccount_id(index=0)
            
        position = self.get_position(subaccount_id, market_id)
        
        # Simple market making strategy: place orders at current bid/ask
        result = StrategyResult()
        
        # Check if we can place a buy order (if not at max position for longs)
        can_buy = not position or position.get("quantity", Decimal("0")) < self.max_position
        
        # Check if we can place a sell order (if not at max position for shorts)
        can_sell = not position or abs(position.get("quantity", Decimal("0"))) < self.max_position
        
        if can_buy:
            buy_order = Order(
                market_id=market_id,
                subaccount_id=subaccount_id,
                order_side=Side.BUY,
                price=bid,  # Place at current bid
                quantity=self.order_size,
                market_type=market.market_type,
                fee_recipient=self.fee_recipient or ""
            )
            
            # Add margin for derivative markets
            if market.market_type == MarketType.DERIVATIVE:
                # Simple margin calculation: price * quantity * leverage factor
                leverage_factor = Decimal("0.1")  # 10x leverage (adjust as needed)
                buy_order.margin = bid * self.order_size * leverage_factor
                
            result.orders.append(buy_order)
            self.logger.info(f"Creating BUY order: price={bid}, quantity={self.order_size}")
            
        if can_sell:
            sell_order = Order(
                market_id=market_id,
                subaccount_id=subaccount_id,
                order_side=Side.SELL,
                price=ask,  # Place at current ask
                quantity=self.order_size,
                market_type=market.market_type,
                fee_recipient=self.fee_recipient or ""
            )
            
            # Add margin for derivative markets
            if market.market_type == MarketType.DERIVATIVE:
                # Simple margin calculation: price * quantity * leverage factor
                leverage_factor = Decimal("0.1")  # 10x leverage (adjust as needed)
                sell_order.margin = ask * self.order_size * leverage_factor
                
            result.orders.append(sell_order)
            self.logger.info(f"Creating SELL order: price={ask}, quantity={self.order_size}")
        
        # Store last price
        self.last_prices[market_id] = (bid + ask) / Decimal("2")
        
        return result if result.orders else None