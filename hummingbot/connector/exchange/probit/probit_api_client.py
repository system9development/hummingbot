
from typing import (
    Dict,
    Any,
)
from decimal import Decimal

import time
import json
import base64
from datetime import datetime

import aiohttp


class ProbitAPIClient:

    REST_API_URL = "https://api.probit.com/api/exchange/v1"

    def __init__(self, api_key = '', api_secret = ''):
        self.api_key: str = api_key
        self.api_secret: str = api_secret
        self.auth_token: Dict[str, Any] = None
        self._shared_client: aiohttp.ClientSession = None

    async def _http_client(self) -> aiohttp.ClientSession:
        """
        :returns Shared client session instance
        """
        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()
        return self._shared_client

    async def get_auth_token(self) -> Dict[str, Any]:
        '''
        Retrieves temporary OAuth token for authorized endpoints.
        :returns An auth toke to be used in authorized requests.
        '''
        token_url = 'https://accounts.probit.com/token'


        client = await self._http_client()
        auth_header = 'Basic ' + base64.b64encode(f'{self.api_key}:{self.api_secret}'.encode('utf-8')).decode('utf-8')
        headers = {
            "Authorization": auth_header,
            "Content-Type": "application/json"
        }
        body = json.dumps({'grant_type': 'client_credentials'})

        response = await client.post(token_url, headers=headers, data=body)

        try:
            response_text = await response.text()
            self.auth_token = json.loads(response_text)
            self.auth_token['created_at'] = time.time()
        except Exception as e:
            raise IOError(f"Error parsing data from {token_url}. Error: {str(e)}")
        if response.status != 200:
            raise IOError(f"Error fetching data from {token_url}. HTTP status is {response.status}. "
                          f"Message: {response_text} ")

        return self.auth_token

    async def api_request(self,
                          method: str,
                          path_url: str,
                          params: Dict[str, Any] = None,
                          data: Dict[str, Any] = None,
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
        query_url = f"{self.REST_API_URL}{path_url}"

        if params is None:
            params = {}

        if data:
            data = json.dumps(data)
        else:
            data = ''
        headers = {"Content-Type": "application/json"}

        if is_auth_required:
            auth_token = await self.get_auth_token()
            headers['Authorization'] = 'Bearer ' + auth_token['access_token']

        client = await self._http_client()
        if method == "get":
            response = await client.get(query_url, headers=headers, params=params)
        elif method == "post":
            response = await client.post(query_url, headers=headers, params=params, data=data)
        elif method == "delete":
            response = await client.delete(query_url, headers=headers, params=params)
        else:
            raise NotImplementedError

        try:
            response_text = await response.text()
            parsed_response = json.loads(response_text)
        except Exception as e:
            raise IOError(f"Error parsing data from {query_url}. Error: {str(e)}")
        if response.status != 200:
            raise IOError(f"Error fetching data from {query_url}. HTTP status is {response.status}. "
                          f"Message: {parsed_response} "
                          f"Request params: {params} "
                          f"Request data: {data}")

        return parsed_response

    async def get_ticker_price(self, symbol: str):
        params = {"market_ids": symbol}
        api_response = await self.api_request("get", "/ticker", params=params)
        return api_response['data']

    async def get_order_book(self, symbol: str):
        params = {"market_id": symbol}
        api_response = await self.api_request("get", "/order_book", params=params)
        return api_response['data']

    async def get_recent_trades(self, symbol: str, start_time: datetime, end_time: datetime, limit: int = 1000):
        req_start_time = start_time.isoformat().split('.')[0] + '.000Z'
        req_end_time = end_time.isoformat().split('.')[0] + '.000Z'
        params = {"market_id": symbol, "limit": limit, 'start_time': req_start_time, 'end_time': req_end_time}
        api_response = await self.api_request("get", "/trade", params=params)
        return api_response['data']

    async def ping(self):
        api_response = await self.api_request("get", "/time")
        return api_response

    async def get_markets(self):
        api_response = await self.api_request("get", "/market")
        return api_response['data']

    async def create_order(self, client_order_id: str, symbol: str, side: str, type: str, quantity: Decimal, price: Decimal):
        params = {
            'market_id': symbol,
            'quantity': str(quantity),
            'side': side,
            'type': type
        }
        if client_order_id:
            params['client_order_id'] = client_order_id
        if type == 'limit':
            params['limit_price'] = str(price)
            params['time_in_force'] = 'gtc'
        else:
            params['time_in_force'] = 'ioc'

        api_response = await self.api_request("post", "/new_order", data=params, is_auth_required=True)
        return api_response['data']

    async def get_order(self, symbol: str, order_id: str):
        params = {
            'order_id': order_id,
            'market_id': symbol
        }
        api_response = await self.api_request("get", "/order", params=params, is_auth_required=True)
        return api_response['data']

    async def get_open_orders(self, symbol: str):
        params = {
            'market_id': symbol
        }
        api_response = await self.api_request("get", "/open_order", params=params, is_auth_required=True)
        return api_response['data']

    async def cancel_order(self, symbol: str, order_id: str):
        params = {
            'order_id': order_id,
            'market_id': symbol
        }
        api_response = await self.api_request("post", "/cancel_order", data=params, is_auth_required=True)
        return api_response['data']

    async def get_order_history(self, symbol: str, start_time: datetime, end_time: datetime, limit: int = 100):
        req_start_time = start_time.isoformat().split('.')[0] + '.000Z'
        req_end_time = end_time.isoformat().split('.')[0] + '.000Z'
        params = {"market_id": symbol, "limit": limit, 'start_time': req_start_time, 'end_time': req_end_time}
        api_response = await self.api_request("get", "/order_history", params=params, is_auth_required=True)
        return api_response['data']

    async def get_balance(self):
        api_response = await self.api_request("get", "/balance", is_auth_required=True)
        return api_response['data']

    async def get_trade_history(self, symbol: str, start_time: datetime, end_time: datetime, limit: int = 100):
        req_start_time = start_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        req_end_time = end_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        params = {"market_id": symbol, "limit": limit, 'start_time': req_start_time, 'end_time': req_end_time}
        api_response = await self.api_request("get", "/trade_history", params=params, is_auth_required=True)
        return api_response['data']
