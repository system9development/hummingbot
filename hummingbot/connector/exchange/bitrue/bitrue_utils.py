from hummingbot.core.utils.tracking_nonce import get_tracking_nonce
from hummingbot.core.event.events import (
    OrderType,
    TradeType
)
import re
from typing import (
    Optional,
    Tuple)


TRADING_PAIR_SPLITTER = re.compile(r"^(\w+)(BTC|USDT|XRP|ETH)$")

HBOT_BROKER_ID = "HB-"


def get_new_client_order_id(side: str, trading_pair: str) -> str:
    side = 'S' if side == 'SELL' else 'B'
    return f"{trading_pair}-{side}{get_tracking_nonce()}"


def get_bitrue_trade_type(trade_type: TradeType) -> str:
    hb_to_exchange_mapping = {
        TradeType.SELL: 'SELL',
        TradeType.BUY: 'BUY'}
    return hb_to_exchange_mapping[trade_type]


def get_bitrue_order_type(order_type: OrderType) -> str:
    hb_to_exchange_mapping = {
        OrderType.LIMIT: 'LIMIT',
        OrderType.MARKET: 'MARKET'}
    return hb_to_exchange_mapping[order_type]


def split_trading_pair(trading_pair: str) -> Optional[Tuple[str, str]]:
    try:
        m = TRADING_PAIR_SPLITTER.match(trading_pair)
        return m.group(1), m.group(2)
    # Exceptions are now logged as warnings in trading pair fetcher
    except Exception:
        return None


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
    if split_trading_pair(exchange_trading_pair) is None:
        return None

    base_asset, quote_asset = split_trading_pair(exchange_trading_pair)
    return f"{base_asset}-{quote_asset}"


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    # Binance does not split BASEQUOTE (BTCUSDT)
    return hb_trading_pair.replace("-", "")
