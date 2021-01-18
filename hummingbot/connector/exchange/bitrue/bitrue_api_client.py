
from typing import (
    Dict,
    Any,
)
from decimal import Decimal

import hashlib
import hmac
import time
import json

import aiohttp


class BitrueException(Exception):
    def __init__(self, info):
        self.message = "Unable to fetch Bitrue API..."
        self.info = info

    def __str__(self) -> str:
        return f"{self.message}{self.info}"


class BitrueInternalServerException(BitrueException):
    def __init__(self, info):
        self.message = "Bitrue internal server error..."
        self.info = info


class BitrueRateLimitException(BitrueException):
    def __init__(self, info):
        self.message = "Bitrue rate limit exceeded..."
        self.info = info


class BitrueAPIClient:
    BITRUE_ERRORS = {"default": BitrueException, 503: BitrueInternalServerException, 429: BitrueRateLimitException}
    REST_API_URL = "https://www.bitrue.com/api/v1"

    def __init__(self, api_key = '', api_secret = ''):
        self.api_key: str = api_key
        self.api_secret: str = api_secret
        self._shared_client: aiohttp.ClientSession = None

    async def _http_client(self) -> aiohttp.ClientSession:
        """
        :returns Shared client session instance
        """
        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()
        return self._shared_client

    async def api_request(self,
                          method: str,
                          path_url: str,
                          params: Dict[str, Any] = None,
                          is_auth_required: bool = False) -> Dict[str, Any]:
        """
        Sends an aiohttp request and waits for a response.
        :param method: The HTTP method, e.g. get or post
        :param path_url: The path url or the API end point
        :param params: API request params
        :param is_auth_required: Whether an authentication is required, when True the function will add encrypted
        signature to the request.
        :returns A response in json format.
        """
        url = f"{self.REST_API_URL}{path_url}"

        if params is None:
            params = {}

        if "timestamp" not in params:
            params['timestamp'] = int(time.time() * 1000) + 1000

        params_sorted = sorted([(k, v) for (k, v) in params.items()])
        query_string = '&'.join(["{}={}".format(d[0], d[1]) for d in params_sorted])
        query_url = f"{url}?{query_string}"

        if is_auth_required:
            signature = self._generate_signature(params)
            query_url += f"&signature={signature}"
            headers = {
                "X-MBX-APIKEY": self.api_key,
                "Content-Type": "application/json"}
        else:
            headers = {"Content-Type": "application/json"}

        client = await self._http_client()
        if method == "get":
            response = await client.get(query_url, headers=headers)
        elif method == "post":
            response = await client.post(query_url, headers=headers)
        elif method == "delete":
            response = await client.delete(query_url, headers=headers)
        else:
            raise NotImplementedError

        try:
            response_text = await response.text()
            if response.status != 200:
                info = {}
                info['status'] = response.status
                info['text'] = response_text
                info['url'] = query_url
                if response.status in self.BITRUE_ERRORS:
                    raise self.BITRUE_ERRORS[response.status](info)
                else:
                    raise self.BITRUE_ERRORS['default'](info)
            else:
                parsed_response = json.loads(response_text)
                return parsed_response
        except Exception as e:
            raise IOError(f"Error parsing data from {url}. Error: {str(e)}")
        return parsed_response
        return None

    def _generate_signature(self, params: Dict[str, Any]) -> str:
        params_sorted = sorted([(k, v) for (k, v) in params.items()])
        query_string = '&'.join(["{}={}".format(d[0], d[1]) for d in params_sorted])
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256).hexdigest()

        return signature

    async def get_ticker_price(self, symbol: str):
        params = {"symbol": symbol}
        api_response = await self.api_request("get", "/ticker/price", params=params)
        return api_response

    async def get_order_book(self, symbol: str, limit: int = 1000):
        params = {"symbol": symbol, "limit": limit}
        api_response = await self.api_request("get", "/depth", params=params)
        return api_response

    async def get_recent_trades(self, symbol: str, limit: int = 1000):
        params = {"symbol": symbol, "limit": limit}
        api_response = await self.api_request("get", "/trades", params=params)
        return api_response

    async def ping(self):
        api_response = await self.api_request("get", "/ping")
        return api_response

    async def get_exchange_info(self):
        api_response = await self.api_request("get", "/exchangeInfo")
        return api_response

    async def create_order(self, client_order_id: str, symbol: str, side: str, type: str, quantity: Decimal, price: Decimal):
        params = {
            'newClientOrderId': client_order_id,
            'symbol': symbol,
            'side': side,
            'type': type,
            'quantity': quantity,
            'price': price}
        api_response = await self.api_request("post", "/order", params=params, is_auth_required=True)
        return api_response

    async def get_order(self, symbol: str, order_id: str):
        params = {
            'orderId': order_id,
            'symbol': symbol}
        api_response = await self.api_request("get", "/order", params=params, is_auth_required=True)
        return api_response

    async def cancel_order(self, symbol: str, order_id: str):
        params = {
            'orderId': order_id,
            'symbol': symbol}
        api_response = await self.api_request("delete", "/order", params=params, is_auth_required=True)
        return api_response

    async def get_account(self):
        api_response = await self.api_request("get", "/account", is_auth_required=True)
        return api_response

    async def get_my_trades(self, symbol: str, limit: int = 100):
        params = {'symbol': symbol, 'limit': limit}
        api_response = await self.api_request("get", "/myTrades", params=params, is_auth_required=True)
        return api_response
