#!/usr/bin/env python

import asyncio
from collections import deque, defaultdict
import logging
import time

from typing import (
    Deque,
    Dict,
    List,
    Optional
)

from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
from hummingbot.connector.exchange.probit.probit_api_order_book_data_source import ProbitAPIOrderBookDataSource
from hummingbot.connector.exchange.probit import probit_constants

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class ProbitOrderBookTracker(OrderBookTracker):
    _bobt_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._bobt_logger is None:
            cls._bobt_logger = logging.getLogger(__name__)
        return cls._bobt_logger

    def __init__(self,
                 trading_pairs: Optional[List[str]] = None):
        super().__init__(
            data_source=ProbitAPIOrderBookDataSource(trading_pairs=trading_pairs),
            trading_pairs = trading_pairs
        )
        # self._order_book_diff_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_snapshot_stream: asyncio.Queue = asyncio.Queue()
        self._ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        self._saved_message_queues: Dict[str, Deque[OrderBookMessage]] = defaultdict(lambda: deque(maxlen=1000))

    @property
    def exchange_name(self) -> str:
        return probit_constants.EXCHANGE_NAME

    async def _track_single_book(self, trading_pair: str):
        past_diffs_window: Deque[OrderBookMessage] = deque()
        self._past_diffs_windows[trading_pair] = past_diffs_window

        message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
        order_book: OrderBook = self._order_books[trading_pair]
        last_message_timestamp: float = time.time()
        diff_messages_accepted: int = 0

        while True:
            try:
                message: OrderBookMessage = None
                saved_messages: Deque[OrderBookMessage] = self._saved_message_queues[trading_pair]

                # Process saved messages first if there are any
                if len(saved_messages) > 0:
                    message = saved_messages.popleft()
                else:
                    message = await message_queue.get()

                if message.type is OrderBookMessageType.DIFF:
                    order_book.apply_diffs(message.bids, message.asks, message.update_id)
                    past_diffs_window.append(message)
                    while len(past_diffs_window) > self.PAST_DIFF_WINDOW_SIZE:
                        past_diffs_window.popleft()
                    diff_messages_accepted += 1

                    # Output some statistics periodically.
                    now: float = time.time()
                    if int(now / 60.0) > int(last_message_timestamp / 60.0):
                        self.logger().info("Processed %d order book diffs inside _track_single_book inside probit_order_book_tracker for %s.",
                                           diff_messages_accepted, trading_pair)
                        diff_messages_accepted = 0
                    last_message_timestamp = now
                elif message.type is OrderBookMessageType.SNAPSHOT:
                    past_diffs: List[OrderBookMessage] = list(past_diffs_window)
                    order_book.restore_from_snapshot_and_diffs(message, past_diffs)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f"Unexpected error tracking order book for {trading_pair}.",
                    exc_info=True,
                    app_warning_msg="Unexpected error tracking order book. Retrying after 5 seconds."
                )
                await asyncio.sleep(5.0)
