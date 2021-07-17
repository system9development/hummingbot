#!/usr/bin/env python

import asyncio
import logging
import time

from typing import Optional, List, AsyncIterable, Any
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.probit.probit_auth import ProbitAuth
from hummingbot.connector.exchange.probit import probit_constants
from hummingbot.connector.exchange.probit.probit_websocket import ProbitWebsocket


class ProbitAPIUserStreamDataSource(UserStreamTrackerDataSource):

    WEBSOCKET_TIMEOUT = 30.0

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, probit_auth: ProbitAuth, trading_pairs: Optional[List[str]] = []):
        self._domain: Optional[str] = probit_constants.WS_URI
        self._probit_auth: ProbitAuth = probit_auth
        self._websocket_client: ProbitWebsocket = ProbitWebsocket(auth = self._probit_auth)
        self._trading_pairs = trading_pairs
        # NOTE: _last_recv_time timestamp can be set with either time.time() or server's response timestamp, we are using time.time()
        self._last_recv_time: float = time.time() * 1000.0
        super().__init__()

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def _init_websocket_connection(self):
        await self._websocket_client.connect()
        return self._websocket_client

    # NOTE: Authentication already handled in probit_websocket, this can probably be removed
    async def _authenticate(self):
        await self._websocket_client.send_ws_authorization()
        return

    async def _subscribe_to_channels(self):
        """
        Subscribes to order history, trade history, and balance endpoints using websocket
        Analagous to _listen_to_orders_trades_balances in crypto_com version of this class
        """
        user_data_channels = ['order_history', 'trade_history', 'balance']

        try:
            await self._init_websocket_connection()

        except Exception as e:
            self.logger().info(
                f"Error in _init_websocket_connection: {e}"
            )

        # Subscribing to each of the user data channels
        for channel in user_data_channels:
            try:
                await self._websocket_client.request(type_sub_or_unsub = "subscribe", channel = channel)

            except Exception as e:
                raise e
                self.logger().info(
                    f"Error subscribing to user data channel {channel} with websocket connection...{e}",
                    exc_info = True
                )
        try:
            async for msg in self._websocket_client.on_message():

                self.logger().info(
                    f" Message received inside _subscribe_to_channels, passing to listen_for_user_stream: {msg}"
                )

                yield msg
                self._last_recv_time = time.time() * 1000.0
        except Exception as e:
            raise e
            self.logger().info(
                f"Error receiving messages from websocket user data channel subscriptions...{e}",
                exc_info = True
            )
        finally:
            await self._websocket_client.disconnect()
            await asyncio.sleep(5)

    # Listens for the messages our websocket is subscribed to
    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue) -> AsyncIterable[Any]:
        """
        *required
        This function subscribes on our WS to the order history, trade history, and balances channels and outputs the updates to the queue
        :param ev_loop: async event loop to execute the function in
        :param output: async queue we output received channel messages onto
        """
        while True:
            try:
                async for msg in self._subscribe_to_channels():
                    self.logger().info(
                        f"Message received in listen_for_user_stream: {msg}"
                    )
                    output.put_nowait(msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    f"Unexpected error subscribing to user data channels with Probit Websocket connection... Retrying after {self.WEBSOCKET_TIMEOUT} seconds...",
                    exc_info = True
                )
                await asyncio.sleep(self.WEBSOCKET_TIMEOUT)
