import hashlib
import hmac
import base64
import aiohttp
import logging
import asyncio
from hummingbot.connector.exchange.probit import probit_constants

from hummingbot.logger import HummingbotLogger
from typing import (
    Dict,
    Any,
    Optional
)


class ProbitAuth():
    """
    Auth class for managing user credentials.
    """
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.oauth_token: Optional[str] = None
        self.expires_in: Optional[float] = None

        self.oauth_token_task = asyncio.ensure_future(self.ensure_oauth_token())

    def get_auth_credentials(self):
        return {'api_key': self.api_key, 'api_secret': self.secret_key}

    async def ensure_oauth_token(self):
        try:
            auth_string = f"{self.api_key}:{self.secret_key}"
            auth_string_bytes = auth_string.encode('ascii')
            base64_auth_string_bytes = base64.b64encode(auth_string_bytes)
            base64_message = base64_auth_string_bytes.decode('ascii')

            payload = {
                "grant_type": "client_credentials"
            }

            headers = {
                "Accept": "application/json",
                "Authorization": "Basic " + base64_message,
                "Content-Type": "application/json"
            }

            while True:
                async with aiohttp.ClientSession() as session:
                    try:
                        async with session.post(probit_constants.AUTH_URL, json = payload, headers = headers) as auth_response:
                            resp = await auth_response.json()
                            if "access_token" in resp:
                                self.oauth_token = resp["access_token"]
                                self.expires_in = float(resp["expires_in"]) * 0.75
                                await session.close()
                                await asyncio.sleep(self.expires_in)
                            else:
                                self.logger().error(f"Failed to retrieve access token (retrying in 5 seconds...){await auth_response.text()}")
                                await asyncio.sleep(5)
                    except Exception as e:
                        self.logger().error(f"Websocket error: {str(e)}", exc_info = True)
                        # NOTE: Retry timer as in asyncio.sleep() or an actual timer obj?
                        await asyncio.sleep(5)
        except Exception as e:
            self.logger().error(f"Failed to authenticate websocket... {e}", exc_info=True)
    # This function is used to send the WS auth request before the actual request in probit_websocket

    def generate_auth_dict(
        self,
        path_url: str,
        request_id: int,
        nonce: int,
        data: Dict[str, Any] = None
    ):
        """
        Generates authentication signature and return it in a dictionary along with other inputs
        :return: a dictionary of request info including the request signature
        """

        data = data or {}
        data['method'] = path_url
        data.update({'nonce': nonce, 'api_key': self.api_key, 'id': request_id})

        data_params = data.get('params', {})
        if not data_params:
            data['params'] = {}
        params = ''.join(
            f'{key}{data_params[key]}'
            for key in sorted(data_params)
        )

        payload = f"{path_url}{data['id']}" \
            f"{self.api_key}{params}{data['nonce']}"

        data['sig'] = hmac.new(
            self.secret_key.encode('utf-8'),
            payload.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        return data
