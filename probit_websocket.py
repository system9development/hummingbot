#!/usr/bin/env python
import asyncio
import logging
import websockets
import ujson
import time

from typing import Optional, AsyncIterable, Any
from websockets.exceptions import ConnectionClosed
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.probit.probit_auth import ProbitAuth
from hummingbot.connector.exchange.probit.probit_utils import get_new_client_order_id
from hummingbot.connector.exchange.probit import probit_constants


class ProbitWebsocket():
    MESSAGE_TIMEOUT = probit_constants.MESSAGE_TIMEOUT
    PING_TIMEOUT = probit_constants.PING_TIMEOUT
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, auth: Optional[ProbitAuth] = None):
        self._auth: Optional[ProbitAuth] = auth
        self._isPrivate = True if self._auth is not None else False
        self._WS_URI = probit_constants.WS_URI
        self._client: Optional[websockets.WebSocketClientProtocol] = None

    # Connects to WS connection
    async def connect(self):
        try:
            self._client = await websockets.connect(self._WS_URI)
            return self._client

        except Exception as e:
            self.logger().error(f"Websocket error: {str(e)}", exc_info = True)

    # To disconnect from the WS connection
    async def disconnect(self):
        if self._client is None:
            return

        await self._client.close()

    # TODO: Verify this function is working
    async def _messages(self) -> AsyncIterable[Any]:
        try:
            while True:
                try:
                    raw_msg_str: str = await asyncio.wait_for(self._client.recv(), timeout = self.MESSAGE_TIMEOUT)
                    raw_msg = ujson.loads(raw_msg_str)

                    # TODO: Handle ping messages (No mention in the docs?)

                    yield raw_msg
                except asyncio.TimeoutError:
                    await asyncio.wait_for(self._client.ping(), timeout = self.PING_TIMEOUT)
        except asyncio.TimeoutError:
            self.logger().warning("Websocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await self.disconnect()

    # _emit function takes req. payload from request() for request msg, also renews authentication if needed
    async def _emit(self, payload: Optional[dict] = {}) -> int:

        # NOTE: Assuming this is for hummingbot's temp id before exchange id response?
        # Params don't actually matter, it just returns the timestamp
        id = get_new_client_order_id("buy", "ETH-BTC")

        # If we have auth object, we must send auth WS msg first w/ OAuth token, this auth is good for entire remainder of WS connection
        if self._isPrivate:

            while True:
                # If we have no Oauth token or token has expired, we must fetch new one to send with request
                # NOTE: Since the WS connection is considered
                if self._auth.oauth_token is None or self._auth.expires_at < time.time():
                    await self._auth.get_oauth_token()

                # Creating auth req to send before actual req
                auth_payload = {
                    "type": "authorization",
                    "token": self._auth.oauth_token
                }

                # Sending auth payload
                await self._client.send(ujson.dumps(auth_payload))
                auth_resp = await self._client.recv()
                resp = ujson.loads(auth_resp)

                # TODO: Error handling for auth_payload response
                if "errorcode" in resp:
                    continue
                else:
                    break

        # Sending actual request
        await self._client.send(ujson.dumps(payload))

        # TODO: Error checking to ensure response is successful

        return id

    async def request(self, type_sub_or_unsub: str, channel: str, params: Optional[dict] = {}) -> int:
        req_payload = {
            "type": type_sub_or_unsub,
            "channel": channel
        }

        if params:
            req_payload.update(params)

        print(f"Initial payload:{req_payload}")
        return await self._emit(payload = req_payload)

    # Listens for messages by method
    async def on_message(self) -> AsyncIterable[Any]:
        async for msg in self._messages():
            yield msg


# NOTE: This def is just here for debugging
async def main():

    # Example of unauthenticated req (only channel not needing auth is "marketdata")
    cli_sock = ProbitWebsocket()
    await cli_sock.connect()
    await cli_sock.request(type_sub_or_unsub = "subscribe", channel = "marketdata", params = {"interval": 500})

    # Example of authenticated req
    # NOTE: We must pass a ProbitAuth object into the WS constructor for any authenticated req's

    # cli_sock = ProbitWebsocket(auth = ProbitAuth(api_key = api_key, secret_key = secret_key))
    # await cli_sock.connect()
    # await cli_sock.request(type_sub_or_unsub = "subscribe", channel = "trade_history")

    while True:
        print(await cli_sock._client.recv())

asyncio.run(main())
