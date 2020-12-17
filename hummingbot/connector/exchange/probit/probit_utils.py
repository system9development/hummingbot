from hummingbot.core.utils.tracking_nonce import get_tracking_nonce
from hummingbot.core.event.events import (
    OrderType,
    TradeType
)
from typing import (
    Optional)


def get_new_client_order_id(side: str, trading_pair: str) -> str:
    side = 'S' if side == 'sell' else 'B'
    # The Probit exchanges supports only 16-character length client_order_id,
    # while HB pure_market_making strategy takes the last 16 characters of the client_order_id to determine its creation time.
    # So simple 16-byte timestamp seems t be the only option for client_order_id
    return f"{get_tracking_nonce()}"


def get_probit_trade_type(trade_type: TradeType) -> str:
    hb_to_exchange_mapping = {
        TradeType.SELL: 'sell',
        TradeType.BUY: 'buy'}
    return hb_to_exchange_mapping[trade_type]


def get_probit_order_type(order_type: OrderType) -> str:
    hb_to_exchange_mapping = {
        OrderType.LIMIT: 'limit',
        OrderType.MARKET: 'market'}
    return hb_to_exchange_mapping[order_type]


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
    return exchange_trading_pair


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    return hb_trading_pair
