
from typing import (
    Dict,
    Any,
)

import hashlib
import hmac
import time
import json

import aiohttp


class BitrueAPI:

    REST_API_URL = "https://www.bitrue.com/api/v1"

    def __init__(self, api_key = None, api_secret = ''):
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
                          params: Dict[str, Any] = {},
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

        if len(params) > 0:
            if "timestamp" not in params:
                params['timestamp'] = int(time.time() * 1000)
            params_sorted = sorted([(k, v) for (k, v) in params.items()])
            query_string = '&'.join(["{}={}".format(d[0], d[1]) for d in params_sorted])
            query_url = f"{url}?{query_string}"
        else:
            query_url = url

        if is_auth_required:
            signature = self._generate_signature(params)
            query_url += f"&signature={signature}"
            headers = {
                "X-MBX-APIKEY": self.api_key,
                "Content-Type": "application/json"}
        else:
            headers = {"Content-Type": "application/json"}

        print(query_url)
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
            parsed_response = json.loads(await response.text())
        except Exception as e:
            raise IOError(f"Error parsing data from {url}. Error: {str(e)}")
        if response.status != 200:
            raise IOError(f"Error fetching data from {url}. HTTP status is {response.status}. "
                          f"Message: {parsed_response}")
        # print(f"REQUEST: {method} {path_url} {params}")
        # print(f"RESPONSE: {parsed_response}")
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
