#!/usr/bin/env python
import asyncio
import logging
import time
import pandas as pd

from typing import Optional, List, Dict, Any
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.logger import HummingbotLogger

from hummingbot.connector.exchange.bitrue.bitrue_order_book import BitrueOrderBook

from bitrue.client import Client as BitrueAPIClient


class BitrueAPIOrderBookDataSource(OrderBookTrackerDataSource):

    ORDER_BOOK_SNAPSHOT_TIMEOUT = 5.0
    ORDER_BOOK_DIFF_TIMEOUT = 5.0
    TRADE_TIMEOUT = 5.0
    ERROR_TIMEOUT = 20.0

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pairs: List[str] = None):
        super().__init__(trading_pairs)
        self._trading_pairs: List[str] = trading_pairs
        self._snapshot_msg: Dict[str, any] = {}

        # Bitrue REST API client
        self.bitrue_client: BitrueAPIClient = BitrueAPIClient('', '')

        # This param is used to filter new trades from API response as Bitrue does not support incremental trades updates
        self.last_max_trade_id: Dict[str, int] = {trading_pair: 0 for trading_pair in trading_pairs}

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        tasks = [self.get_last_traded_price(t_pair) for t_pair in trading_pairs]
        results = await safe_gather(*tasks)
        return {t_pair: result for t_pair, result in zip(trading_pairs, results)}

    async def get_last_traded_price(self, trading_pair: str) -> float:
        # Expected output from client request: {'symbol': 'ETHBTC', 'price': '0.033342'}
        api_response = self.bitrue_client.get_ticker_price(trading_pair)
        return float(api_response['price'])

    async def get_snapshot(self, trading_pair: str) -> Dict[str, any]:
        """
        Get whole orderbook
        """
        order_book = self.bitrue_client.get_order_book(symbol=trading_pair, limit=1000)
        # Expected output
        # {'lastUpdateId': 1601302979911, 'bids': [['0.033366', '0.74', []], ['0.033365', '9.461', []]], 'asks': [['0.033369', '1.295', []], ['0.033370', '9.36', []]]}

        return order_book

    async def get_recent_trades(self, trading_pair: str) -> Dict[str, any]:
        """
        Get recent trades from REST API
        """
        recent_trades = self.bitrue_client.get_recent_trades(symbol=trading_pair, limit=1000)
        # Expected response
        # [{'id': 54866489, 'price': '0.0331220000000000', 'qty': '3.9570000000000000', 'time': 1601385563186, 'isBuyerMaker': True, 'isBestMatch': True}, {'id': 54866488, 'price': '0.0331220000000000', 'qty': '3.4310000000000000', 'time': 1601385563020, 'isBuyerMaker': True, 'isBestMatch': True}]

        return recent_trades

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        snapshot: Dict[str, Any] = await self.get_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = BitrueOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        order_book = self.order_book_create_function()
        order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
        return order_book

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Fetches orderbook snapshots
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        snapshot: Dict[str, any] = await self.get_snapshot(trading_pair)
                        snapshot_timestamp: int = snapshot["lastUpdateId"]
                        snapshot_msg: OrderBookMessage = BitrueOrderBook.snapshot_message_from_exchange(
                            snapshot,
                            snapshot_timestamp,
                            metadata={"trading_pair": trading_pair}
                        )
                        output.put_nowait(snapshot_msg)
                        self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                        # Be careful not to go above API rate limits.
                        await asyncio.sleep(self.ORDER_BOOK_DIFF_TIMEOUT)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.logger().network(
                            "Unexpected error.",
                            exc_info=True,
                            app_warning_msg="Unexpected error with REST API request..."
                        )
                        await asyncio.sleep(self.ERROR_TIMEOUT)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(self.ERROR_TIMEOUT)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Fetches orderbook snapshots
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        snapshot: Dict[str, any] = await self.get_snapshot(trading_pair)
                        snapshot_timestamp: int = snapshot["lastUpdateId"]
                        snapshot_msg: OrderBookMessage = BitrueOrderBook.snapshot_message_from_exchange(
                            snapshot,
                            snapshot_timestamp,
                            metadata={"trading_pair": trading_pair}
                        )
                        output.put_nowait(snapshot_msg)
                        self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                        # Be careful not to go above API rate limits.
                        await asyncio.sleep(self.ORDER_BOOK_SNAPSHOT_TIMEOUT)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.logger().network(
                            "Unexpected error with REST API request...",
                            exc_info=True
                        )
                        await asyncio.sleep(self.ERROR_TIMEOUT)
                this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                delta: float = next_hour.timestamp() - time.time()
                await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(self.ERROR_TIMEOUT)

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):

        while True:
            try:
                for trading_pair in self._trading_pairs:
                    trades: List[Dict[str, any]] = await self.get_recent_trades(trading_pair)
                    max_trade_id = 0
                    for trade in trades[::-1]:
                        # Process only trades that haven't been seen before
                        if trade['id'] > self.last_max_trade_id[trading_pair]:
                            trade: Dict[Any] = trade
                            trade_timestamp: int = trade['time']
                            trade_msg: OrderBookMessage = BitrueOrderBook.trade_message_from_exchange(
                                trade,
                                trade_timestamp,
                                metadata={"trading_pair": trading_pair}
                            )
                            output.put_nowait(trade_msg)
                            # Update last_max_trade_id
                            max_trade_id = max(max_trade_id, trade['id'])
                    self.last_max_trade_id[trading_pair] = max(max_trade_id, self.last_max_trade_id[trading_pair])

                await asyncio.sleep(self.TRADE_TIMEOUT)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with REST API request...",
                                    exc_info=True)
                await asyncio.sleep(self.ERROR_TIMEOUT)
