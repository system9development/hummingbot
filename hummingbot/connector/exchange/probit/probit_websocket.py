#!/usr/bin/env python
import asyncio
import logging
import websockets
import ujson
import time

from typing import Optional, AsyncIterable, Any, Dict
from websockets.exceptions import ConnectionClosed
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.probit.probit_auth import ProbitAuth
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
        self._WS_URI = probit_constants.WS_URI
        self._client: Optional[websockets.WebSocketClientProtocol] = None

    async def connect(self):
        while True:
            try:
                self._client = await websockets.connect(self._WS_URI)
                if isinstance(self._auth, ProbitAuth) and isinstance(self._client, websockets.WebSocketClientProtocol):
                    await self.send_ws_authorization()
                return self._client

            except Exception as e:
                self.logger().error(f"Websocket error: {str(e)}", exc_info = True)
                await asyncio.sleep(3)
                continue

    # To disconnect from the WS connection
    async def disconnect(self):

        if self._client is None:
            return
        await self._client.close()

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

    # _emit function takes req. payload from request() and sends it to exchange
    # also sends any needed auth msges prior
    async def _emit(self, payload: Optional[Dict] = None) -> None:
        await self._client.send(ujson.dumps(payload))

    # request function formats payload and hands off to _emit function
    async def request(self, type_sub_or_unsub: str, channel: str, params: Optional[Dict] = None) -> int:
        req_payload = {
            "type": type_sub_or_unsub,
            "channel": channel
        }
        if params:
            req_payload.update(params)
        self.logger().debug(f"Initial payload:{req_payload}")
        return await self._emit(payload = req_payload)

    # Listens for messages by method
    async def on_message(self) -> AsyncIterable[Any]:
        async for msg in self._messages():
            yield msg

    # Retrieves OAuth token over REST /token API if we need to remain updated
    async def _get_oauth_token(self, timeout: float = 30):
        start_time = time.time()
        while self._auth.oauth_token is None and time.time() - start_time < 30:
            await asyncio.sleep(0.1)
        if self._auth.oauth_token is None:
            raise Exception("Failed to retrieve oauth token...")
        else:
            return self._auth.oauth_token

    # Sends message with oauth token prior to subscribing on authorized channels
    async def send_ws_authorization(self):
        try:
            # Auth WS Request structure
            auth_payload = {
                "type": "authorization",
                "token": await self._get_oauth_token()
            }

            # Sending auth payload and recving response
            await self._client.send(ujson.dumps(auth_payload))
            resp = ujson.loads(await self._client.recv())

            if resp['result'] == "ok":
                return
            else:
                raise Exception(f"Invalid argument when authenticating websocket (Websocket already authorized?).... {resp}")
        except Exception as e:
            self.logger().error(f"Failed to send authorization message to probit websocket... {e}", exc_info=True)
