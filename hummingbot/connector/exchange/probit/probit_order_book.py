#!/usr/bin/env python

import logging

from typing import (
    Optional,
    Dict,
    List,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessage, OrderBookMessageType
)
from hummingbot.core.event.events import TradeType

from decimal import Decimal

_logger = None


class ProbitOrderBook(OrderBook):
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _logger
        if _logger is None:
            _logger = logging.getLogger(__name__)
        return _logger

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: List[Dict[str, any]],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:

        bids = [[Decimal(i['price']), Decimal(i['quantity'])] for i in msg if i['side'] == 'buy']
        asks = [[Decimal(i['price']), Decimal(i['quantity'])] for i in msg if i['side'] == 'sell']
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": metadata["trading_pair"],
            "update_id": timestamp,
            "bids": bids,
            "asks": asks
        }, timestamp=timestamp * 1e-3)

    @classmethod
    def trade_message_from_exchange(cls,
                                    msg: Dict[str, any],
                                    timestamp: float,
                                    metadata: Optional[Dict] = None):
        if metadata:
            msg.update(metadata)

        return OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": metadata["trading_pair"],
            "trade_type": float(TradeType.SELL.value) if msg["side"] == "sell" else float(TradeType.BUY.value),
            "trade_id": msg["id"],
            "update_id": timestamp,
            "price": float(msg["price"]),
            "amount": float(msg["quantity"])
        }, timestamp=timestamp * 1e-3)
