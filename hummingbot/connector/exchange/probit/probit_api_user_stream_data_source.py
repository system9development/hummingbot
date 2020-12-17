#!/usr/bin/env python

import asyncio
import logging
from typing import Optional, List, AsyncIterable, Any
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.probit.probit_auth import ProbitAuth


class ProbitAPIUserStreamDataSource(UserStreamTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, probit_auth: ProbitAuth, trading_pairs: Optional[List[str]] = []):
        self._probit_auth: ProbitAuth = probit_auth
        self._trading_pairs = trading_pairs
        # Probit exchange doesn't have a Websocket API. Set it ready immidiately.
        self._last_recv_time: float = 1
        super().__init__()

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue) -> AsyncIterable[Any]:
        """
        *required
        This function does nothing as Probit exchange doesn't have a Websocket API
        """

        while True:
            try:
                await asyncio.sleep(30.0)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error", exc_info=True
                )
                await asyncio.sleep(30.0)
