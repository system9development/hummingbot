#!/usr/bin/env python
import asyncio
import logging
import time
import pandas as pd
import math
from datetime import datetime, timezone

from typing import Optional, List, Dict, Any
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.logger import HummingbotLogger

from hummingbot.connector.exchange.probit.probit_order_book import ProbitOrderBook
from hummingbot.connector.exchange.probit.probit_api_client import ProbitAPIClient
from hummingbot.connector.exchange.probit import probit_utils
from hummingbot.connector.exchange.probit.probit_websocket import ProbitWebsocket


class ProbitAPIOrderBookDataSource(OrderBookTrackerDataSource):

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

        # Probit REST API client
        self.probit_client: ProbitAPIClient = ProbitAPIClient('', '')

        # This param is used to filter new trades from API response as Probit does not support incremental trades updates
        if trading_pairs:
            self.last_trade_request_time: Dict[str, datetime] = {trading_pair: datetime.utcnow() for trading_pair in trading_pairs}
        else:
            self.last_trade_request_time: Dict[str, datetime] = None

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        tasks = [self.get_last_traded_price(t_pair) for t_pair in trading_pairs]
        results = await safe_gather(*tasks)
        return {t_pair: result for t_pair, result in zip(trading_pairs, results)}

    async def get_last_traded_price(self, trading_pair: str) -> float:
        # Expected output from client request: {'symbol': 'ETHBTC', 'price': '0.033342'}
        api_response = await self.probit_client.get_ticker_price(probit_utils.convert_to_exchange_trading_pair(trading_pair))
        return float(api_response[0]['last'])

    async def get_snapshot(self, trading_pair: str) -> Dict[str, any]:
        """
        Get whole orderbook
        """

        order_book = await self.probit_client.get_order_book(symbol=probit_utils.convert_to_exchange_trading_pair(trading_pair))
        # Expected output
        # {'lastUpdateId': 1601302979911, 'bids': [['0.033366', '0.74', []], ['0.033365', '9.461', []]], 'asks': [['0.033369', '1.295', []], ['0.033370', '9.36', []]]}

        return order_book

    async def get_recent_trades(self, trading_pair: str) -> Dict[str, any]:
        """
        Get recent trades from REST API
        """
        start_time = self.last_trade_request_time[trading_pair]
        end_time = datetime.utcnow()
        recent_trades = await self.probit_client.get_recent_trades(symbol=probit_utils.convert_to_exchange_trading_pair(trading_pair), start_time=start_time, end_time=end_time, limit=1000)
        self.last_trade_request_time[trading_pair] = end_time
        # Expected response
        # [{'id': 54866489, 'price': '0.0331220000000000', 'qty': '3.9570000000000000', 'time': 1601385563186, 'isBuyerMaker': True, 'isBestMatch': True}, {'id': 54866488, 'price': '0.0331220000000000', 'qty': '3.4310000000000000', 'time': 1601385563020, 'isBuyerMaker': True, 'isBestMatch': True}]
        return recent_trades

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        snapshot: Dict[str, Any] = await self.get_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = ProbitOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair, 'update_id': str(time.time())}
        )
        order_book = self.order_book_create_function()
        order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
        return order_book

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Fetches orderbook diffs
        """

        # while True:
        #     try:
        #         for trading_pair in self._trading_pairs:
        #             try:
        #                 snapshot: Dict[str, any] = await self.get_snapshot(trading_pair)
        #                 snapshot_timestamp: float = time.time()
        #                 snapshot_msg: OrderBookMessage = ProbitOrderBook.snapshot_message_from_exchange(
        #                     snapshot,
        #                     snapshot_timestamp,
        #                     metadata={"trading_pair": trading_pair}
        #                 )
        #                 output.put_nowait(snapshot_msg)
        #                 self.logger().debug(f"Saved order book snapshot for {trading_pair}")
        #                 # Be careful not to go above API rate limits.
        #                 await asyncio.sleep(self.ORDER_BOOK_DIFF_TIMEOUT)
        #             except asyncio.CancelledError:
        #                 raise
        #             except Exception as e:
        #                 self.logger().network(
        #                     "Unexpected error.",
        #                     exc_info=True,
        #                     app_warning_msg=f"Unexpected error with REST API request: {e}"
        #                 )
        #                 await asyncio.sleep(self.ERROR_TIMEOUT)

        #     except asyncio.CancelledError:
        #         raise
        #     except Exception:
        #         self.logger().error("Unexpected error.", exc_info=True)
        #         await asyncio.sleep(self.ERROR_TIMEOUT)

        # First WS message has the initial snapshot, all next messages have diffs
        # So just ignore the snapshot, continue outputting the diffs?

        # Should we use a separate WS for each pair?
        # Looks like this method only needs to handle one subscription at a time?

        while True:
            try:
                cli_sock = ProbitWebsocket()
                await cli_sock.connect()

                # If response's "reset"(bool) param == false, it's diff data
                # Can we use this to get only the diffs?
                # NOTE: That's what we did ^

                # Check for if resp[status] == "ok"
                for pair in self._trading_pairs:

                    params = {
                        "market_id": pair,
                        "filter": ["order_books"]
                    }

                    # Subscribing to each trading pair
                    await cli_sock.request(type_sub_or_unsub = "subscribe", channel = "marketdata", params = params)

                    # NOTE: Using aiohttp, the response fields should be automatically parsed
                    async for response in cli_sock.on_message():

                        # If "reset" field of response == True it's a snapshot and not a diff, ignore it
                        if response["reset"] is True:
                            continue
                        else:
                            order_book_diff = response["order_books"]

                            # Setting timestamp as current time - lag on server side
                            timestamp: int = math.floor(time.time() - order_book_diff["lag"])

                            # Generating OrderBookMessage
                            orderbook_message: OrderBookMessage = ProbitOrderBook.snapshot_message_from_exchange(
                                order_book_diff,
                                timestamp,
                                metadata = {"trading_pair": response["market_id"]}
                            )

                            # Outputting OrderBookMessage to queue
                            output.put_nowait(orderbook_message)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Error with Probit WebSocket connection...",
                    exc_info = True,
                    app_warning_msg = f"Error with Probit connection, retrying in {self.ERROR_TIMEOUT} seconds..."
                )
                await asyncio.sleep(self.ERROR_TIMEOUT)
            finally:
                await cli_sock.disconnect()

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Fetches orderbook snapshots
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        snapshot: Dict[str, any] = await self.get_snapshot(trading_pair)
                        snapshot_timestamp: float = time.time()
                        snapshot_msg: OrderBookMessage = ProbitOrderBook.snapshot_message_from_exchange(
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
                    for trade in trades[::-1]:
                        trade: Dict[Any] = trade
                        trade_timestamp: float = time.mktime(datetime.strptime(trade['time'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).timetuple())
                        trade_msg: OrderBookMessage = ProbitOrderBook.trade_message_from_exchange(
                            trade,
                            trade_timestamp,
                            metadata={"trading_pair": trading_pair}
                        )
                        output.put_nowait(trade_msg)

                await asyncio.sleep(self.TRADE_TIMEOUT)
            except asyncio.CancelledError:
                raise
            except Exception:
                # NOTE: Error thrown in hummingbot here when trying to cancel order
                self.logger().error("Unexpected error with REST API request...",
                                    exc_info=True)
                await asyncio.sleep(self.ERROR_TIMEOUT)
