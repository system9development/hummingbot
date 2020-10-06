from hummingbot.core.utils.tracking_nonce import get_tracking_nonce
from hummingbot.core.event.events import (
    OrderType,
    TradeType
)
from bitrue.client import Client as BitrueAPIClient

HBOT_BROKER_ID = "HBOT-"


def get_new_client_order_id(side: str, trading_pair: str) -> str:
    return f"{HBOT_BROKER_ID}{side}-{trading_pair}-{get_tracking_nonce()}"


def get_bitrue_trade_type(trade_type: TradeType):
    hb_to_exchange_mapping = {
        TradeType.SELL: BitrueAPIClient.SIDE_SELL,
        TradeType.BUY: BitrueAPIClient.SIDE_BUY}
    return hb_to_exchange_mapping[trade_type]


def get_bitrue_order_type(order_type: OrderType):
    hb_to_exchange_mapping = {
        OrderType.LIMIT: BitrueAPIClient.ORDER_TYPE_LIMIT,
        OrderType.MARKET: BitrueAPIClient.ORDER_TYPE_MARKET}
    return hb_to_exchange_mapping[order_type]
