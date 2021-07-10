import hashlib
import hmac
import ujson
import time
import base64
import aiohttp
import logging

from hummingbot.connector.exchange.probit.probit_constants import AUTH_URL
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
        self.oauth_token = None
        self.expires_at: float = None

    def get_auth_credentials(self):
        return {'api_key': self.api_key, 'api_secret': self.secret_key}

    # NOTE: We can only retrieve oauth_token over REST, but it's used with either REST or WS API
    async def get_oauth_token(self):

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
            async with aiohttp.ClientSession(json_serialize = ujson.dumps) as session:
                try:
                    async with session.post(AUTH_URL, json = payload, headers = headers) as auth_response:
                        resp = await auth_response.json()
                        print(f"Auth response: {resp}")

                        if "access_token" in resp:
                            self.oauth_token = resp["access_token"]
                            self.expires_at = time.time() + float(resp["expires_in"])
                            await session.close()
                            return
                        else:
                            # self._logger().info(f"Some type of error in retrieiving new oauth token: {resp}")
                            continue
                except Exception as e:
                    # self._logger().error(f"Websocket error: {str(e)}", exc_info = True)
                    print(e)

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
