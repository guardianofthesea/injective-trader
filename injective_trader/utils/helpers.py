from typing import Dict, Any, List
from decimal import Decimal
from pyinjective.async_client import BinaryOptionMarket
from pyinjective.composer import SpotMarket, DerivativeMarket, Composer
from base64 import b64decode


def order_to_human_readable(order: Dict, market: SpotMarket | DerivativeMarket) -> Dict[str, str | Decimal]:
    return {
        "cid": order.get("cid", None),
        "status": order["status"],
        "order_hash": order["orderHash"],
        "market_id": order["marketId"],
        "order_type": order["orderType"],
        "trigger_price": market.price_from_extended_chain_format(order["triggerPrice"]),
        "fee_recipient": order["feeRecipient"],
        "subaccount_id": order["subaccountId"],
        "fillable": market.quantity_from_extended_chain_format(Decimal(order["fillable"])),
        "price": market.price_from_extended_chain_format(Decimal(order["price"])),
        "quantity": market.quantity_from_extended_chain_format(Decimal(order["quantity"])),
    }


def position_to_human_readable(position: Dict, market: SpotMarket) -> Dict[str, str | Decimal]:
    return {
        #  'marketId': 'market id',
        "market_id": position["marketId"],
        #  'subaccountId': 'subaccount id'}]
        "subaccount_id": position["subaccountId"],
        #  'isLong': False,
        "isLong": position["isLong"],
        #  'margin': '7535349259299236781936632111',
        "margin": market.notional_from_extended_chain_format(Decimal(position["margin"])),
        #  'quantity': '1979750000000000000000',
        "quantity": market.quantity_from_extended_chain_format(Decimal(position["quantity"])),
        #  'entryPrice': '11416356429355375178053351',
        "entry_price": market.price_from_extended_chain_format(Decimal(position["entryPrice"])),
        # 'cumulativeFundingEntry': '1165709584725390947363969',
        "cumulative_funding_entry": market.notional_from_extended_chain_format(Decimal(position["cumulativeFunidngEntry"])),
    }


def spot_trade_to_human_readable(spot_trade: Dict, market: SpotMarket) -> Dict[str, str | Decimal]:
    return {
        "cid": spot_trade.get("cid", None),
        # 'executionType': 'LimitMatchRestingOrder',
        "executionType": spot_trade["executionType"],
        # 'fee': '-1604700000000000000000',
        "fee": market.notional_from_extended_chain_format(Decimal(spot_trade["fee"])),
        #  'feeRecipientAddress': 'fee recipient',
        "feeRecipientAddress": spot_trade["feeRecipientAddress"],
        #  'isBuy': False,
        "isBuy": spot_trade["isBuy"],
        #  'marketId': 'market id',
        "marketId": spot_trade["marketId"],
        #  'orderHash': 'order hash',
        "orderHash": spot_trade["orderHash"],
        #  'price': '2674500000',
        "price": market.price_from_extended_chain_format(Decimal(spot_trade["price"])),
        #  'quantity': '10000000000000000000000000000000000',
        "quantity": market.quantity_from_extended_chain_format(Decimal(spot_trade["quantity"])),
        #  'subaccountId': 'subaccount id',
        "subaccountId": spot_trade["subaccountId"],
        #  'tradeId': 'trade id'
        "tradeId": spot_trade["tradeId"],
    }


def derivative_trade_to_human_readable(derivative_trade: Dict, market: DerivativeMarket):
    return {
        #  'cid': 'cid',
        "cid": derivative_trade.get("cid", None),
        #  'executionType': 'LimitMatchNewOrder',
        "execution_type": derivative_trade["executionType"],
        #  'fee': '621918900000000000000',
        "fee": market.notional_from_extended_chain_format(Decimal(derivative_trade["fee"])),
        #  'feeRecipientAddress': 'fee recipient',
        "fee_recipient_address": derivative_trade["feeRecipientAddress"],
        #  'isBuy': True,
        "is_buy": derivative_trade["isBuy"],
        #  'marketId': 'market id',
        "market_id": derivative_trade["marketId"],
        #  'orderHash': 'order hash',
        "order_hash": derivative_trade["orderHash"],
        #  'payout': '424017498578031916218037',
        "payout": market.notional_from_extended_chain_format(Decimal(derivative_trade["payout"])),
        #  'positionDelta': {'executionMargin': '425227588888888888888889',
        #                    'executionPrice': '22291000000000000000000000',
        #                    'executionQuantity': '186000000000000000',
        #                    'isLong': True},
        "executionMargin": market.notional_from_extended_chain_format(Decimal(derivative_trade["executionMargin"])),
        "executionPrice": market.price_from_extended_chain_format(Decimal(derivative_trade["executionPrice"])),
        "executionQuantity": market.quantity_from_extended_chain_format(Decimal(derivative_trade["executionQuantity"])),
        "is_long": derivative_trade["isLong"],
        #  'subaccountId': 'subaccount id',
        "subaccount_id": market.notional_from_extended_chain_format(Decimal(derivative_trade["subaccountId"])),
        #  'tradeId': 'trade id'}]
        "trader_id": derivative_trade["tradeId"],
    }


def create_orders_to_cancel(composer: Composer, market_id: str, subaccount_id: str, order_hash: str):
    return composer.order_data_without_mask(
        market_id=market_id,
        subaccount_id=subaccount_id,
        order_hash=order_hash,
    )


def order_hash_from_chain_format(order_hash: str) -> str:
    return "0x" + b64decode(order_hash).hex()


def update_order_book_to_human_readable(orderbook_update: Dict, market: SpotMarket | DerivativeMarket | BinaryOptionMarket) -> Dict[str, Any]:
    orderbook = orderbook_update["orderbook"]
    new_buy_levels = [
        {
            "p": Decimal(market.price_from_extended_chain_format(Decimal(buy_level["p"]))),
            "q": Decimal(market.quantity_from_extended_chain_format(Decimal(buy_level["q"]))),
        }
        for buy_level in orderbook["buyLevels"]
    ]
    new_sell_levels = [
        {
            "p": Decimal(market.price_from_extended_chain_format(Decimal(sell_level["p"]))),
            "q": Decimal(market.quantity_from_extended_chain_format(Decimal(sell_level["q"]))),
        }
        for sell_level in orderbook["sellLevels"]
    ]
    seq = orderbook_update["seq"]
    return {
        "orderbook": {
            "buyLevels": new_buy_levels,
            "sellLevels": new_sell_levels,
            "marketId": orderbook["marketId"],
        },
        "seq": seq,
    }


def update_indexer_order_book_to_human_readable(orderbook_update: Dict, market: SpotMarket | DerivativeMarket | BinaryOptionMarket) -> Dict[str, Any]:
    market_id= orderbook_update["marketId"]
    orderbook = orderbook_update["orderbook"]
    new_buy_levels = [
        {
            "p": Decimal(market.price_from_chain_format(Decimal(buy_level["price"]))),
            "q": Decimal(market.quantity_from_chain_format(Decimal(buy_level["quantity"]))),
        }
        for buy_level in orderbook["buys"]
    ]
    new_sell_levels = [
        {
            "p": Decimal(market.price_from_chain_format(Decimal(sell_level["price"]))),
            "q": Decimal(market.quantity_from_chain_format(Decimal(sell_level["quantity"]))),
        }
        for sell_level in orderbook["sells"]
    ]
    sequence = orderbook["sequence"]
    return {
        "orderbook": {
            "buyLevels": new_buy_levels,
            "sellLevels": new_sell_levels,
            "marketId": market_id
        },
        "seq": sequence,
    }


def order_create_validator(orders_to_create: List, error_code: Dict[str, str], logger):
    """
    Validate the order list according to the specific rules for the market.
    """
    failed = False
    for order in orders_to_create:
        if order and order in error_code:  # Check if element is not empty
            logger.critical(f"Order failed simulation with error code: {error_code[order]}")
            failed = True
    return failed


def order_cancel_validator(orders_to_create: List[bool], logger):
    if any(not success for success in orders_to_create):
        logger.critical("Order cancel failed simulations")
        return True
    else:
        logger.critical("All order cancel simulations passed")
        return False


def return_orders_states_after_sim(sim_responses: List[Dict[str, List]], error_code: Dict[str, str], logger):
    # FIXME this is a temporary solution
    # Check if keys exist in sim_response and set default empty lists
    create_succededs = []
    cancels_succeededs = []
    for sim_response in sim_responses:
        spot_create_response = sim_response.get("spotOrderHashes", [])
        spot_cancel_response = sim_response.get("spotCancelSuccess", [])
        derivative_create_response = sim_response.get("derivativeOrderHashes", [])
        derivative_cancel_response = sim_response.get("derivativeCancelSuccess", [])

        # Check if any orders failed for spot and derivative
        cancels_suceeded, create_suceeded = False, False
        if len(spot_create_response) > 0:
            if not order_create_validator(spot_create_response, error_code, logger):
                create_suceeded = True
        if len(spot_cancel_response) > 0:
            if not order_cancel_validator(spot_cancel_response, logger):
                cancels_suceeded = True
        if len(derivative_cancel_response) > 0:
            if not order_create_validator(derivative_create_response, error_code, logger):
                cancels_suceeded = True

        if len(derivative_create_response) > 0:
            if not order_cancel_validator(derivative_cancel_response, logger):
                create_suceeded = True
        create_succededs.append(create_suceeded)
        cancels_succeededs.append(cancels_suceeded)
    return create_succededs, cancels_succeededs

def get_correct_sequence_number(error_str:str)->int:
    correct_sequence = int(error_str.split("expected ")[1].split()[0][:-1])
    return correct_sequence
