from os.path import join, realpath
import sys; sys.path.insert(0, realpath(join(__file__, "../../../../../")))
import asyncio
import logging
from decimal import Decimal
import unittest
import contextlib
import time
from datetime import datetime
import os
from typing import List, Optional
from unittest import mock
import conf
import math
import copy

from hummingbot.logger import HummingbotLogger

from hummingbot.core.clock import Clock, ClockMode
from hummingbot.logger.struct_logger import METRICS_LOG_LEVEL
from hummingbot.core.utils.async_utils import safe_gather, safe_ensure_future
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketEvent,
    OrderFilledEvent,
    OrderType,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
    OrderCancelledEvent
)
from hummingbot.model.sql_connection_manager import (
    SQLConnectionManager,
    SQLConnectionType
)
from hummingbot.model.market_state import MarketState
from hummingbot.model.order import Order
from hummingbot.model.trade_fill import TradeFill
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.connector.exchange.probit.probit_market import ProbitMarket

from test.integration.humming_web_app import HummingWebApp
from . import fixture

logging.basicConfig(level=METRICS_LOG_LEVEL)
API_MOCK_ENABLED = conf.mock_api_enabled is not None and conf.mock_api_enabled.lower() in ['true', 'yes', '1']
API_KEY = "XXX" if API_MOCK_ENABLED else conf.probit_api_key
API_SECRET = "YYY" if API_MOCK_ENABLED else conf.probit_secret_key
BASE_API_URL = "api.probit.com"
AUTH_TOKEN_URL = "accounts.probit.com"

# Trade id increment param required for new trades tracking
trade_id_inc = 1


class ProbitMarketUnitTest(unittest.TestCase):
    events: List[MarketEvent] = [
        MarketEvent.BuyOrderCompleted,
        MarketEvent.SellOrderCompleted,
        MarketEvent.OrderFilled,
        MarketEvent.TransactionFailure,
        MarketEvent.BuyOrderCreated,
        MarketEvent.SellOrderCreated,
        MarketEvent.OrderCancelled,
        MarketEvent.OrderFailure
    ]
    connector: ProbitMarket
    event_logger: EventLogger
    trading_pair = "XRP-USDT"
    base_token, quote_token = trading_pair.split("-")
    stack: contextlib.ExitStack

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @classmethod
    def setUpClass(cls):
        cls.ev_loop = asyncio.get_event_loop()
        print('API_MOCK_ENABLED:', API_MOCK_ENABLED)
        if API_MOCK_ENABLED:
            cls.web_app = HummingWebApp.get_instance()
            cls.web_app.add_host_to_mock(BASE_API_URL, [])
            cls.web_app.add_host_to_mock(AUTH_TOKEN_URL, [])
            cls.web_app.start()
            cls.ev_loop.run_until_complete(cls.web_app.wait_til_started())
            cls._patcher = mock.patch("aiohttp.client.URL")
            cls._url_mock = cls._patcher.start()
            cls._url_mock.side_effect = cls.web_app.reroute_local

            cls.web_app.update_response("post", AUTH_TOKEN_URL, "/token", fixture.NEW_TOKEN)
            cls.web_app.update_response("get", BASE_API_URL, "/api/exchange/v1/trade_history", fixture.TRADE_HISTORY)
            cls.web_app.update_response("get", BASE_API_URL, "/api/exchange/v1/ticker", fixture.TICKER)
            cls.web_app.update_response("get", BASE_API_URL, "/api/exchange/v1/trade", fixture.RECENT_TRADES)
            cls.web_app.update_response("get", BASE_API_URL, "/api/exchange/v1/time", fixture.PING)
            cls.web_app.update_response("get", BASE_API_URL, "/api/exchange/v1/market", fixture.MARKETS)
            cls.web_app.update_response("get", BASE_API_URL, "/api/exchange/v1/order_book", fixture.ORDER_BOOK)
            cls.web_app.update_response("get", BASE_API_URL, "/api/exchange/v1/balance", fixture.BALANCE)
            cls.web_app.update_response("post", BASE_API_URL, "/api/exchange/v1/new_order", fixture.CREATE_ORDER)
            cls.web_app.update_response("get", BASE_API_URL, "/api/exchange/v1/order", fixture.GET_ORDER)
            cls.web_app.update_response("post", BASE_API_URL, "/api/exchange/v1/cancel_order", fixture.CANCEL_ORDER_RESPONSE)

        cls.clock: Clock = Clock(ClockMode.REALTIME)
        cls.connector: ProbitMarket = ProbitMarket(
            probit_api_key=API_KEY,
            probit_api_secret=API_SECRET,
            trading_pairs=[cls.trading_pair],
            trading_required=True
        )
        cls.logger().info("Initializing Probit market... this will take about a minute.")
        cls.clock.add_iterator(cls.connector)
        cls.stack: contextlib.ExitStack = contextlib.ExitStack()
        cls._clock = cls.stack.enter_context(cls.clock)

        cls.ev_loop.run_until_complete(cls.wait_til_ready())
        safe_ensure_future(cls.clock.run())
        cls.logger().info("Ready.")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.stack.close()
        if API_MOCK_ENABLED:
            cls.web_app.stop()
            cls._patcher.stop()

    @classmethod
    async def wait_til_ready(cls, connector = None):
        if connector is None:
            connector = cls.connector
        while True:
            now = time.time()
            next_iteration = now // 1.0 + 1
            if connector.ready:
                break
            else:
                await cls._clock.run_til(next_iteration)
            await asyncio.sleep(1.0)

    def setUp(self):
        self.db_path: str = realpath(join(__file__, "../probit_connector_test.sqlite"))
        try:
            os.unlink(self.db_path)
        except FileNotFoundError:
            pass

        self.event_logger = EventLogger()
        for event_tag in self.events:
            self.connector.add_listener(event_tag, self.event_logger)

    def tearDown(self):
        for event_tag in self.events:
            self.connector.remove_listener(event_tag, self.event_logger)
        self.event_logger = None

    async def run_parallel_async(self, *tasks):
        future: asyncio.Future = safe_ensure_future(safe_gather(*tasks))
        while not future.done():
            now = time.time()
            next_iteration = now // 1.0 + 1
            await self._clock.run_til(next_iteration)
            await asyncio.sleep(1.0)
        return future.result()

    def run_parallel(self, *tasks):
        return self.ev_loop.run_until_complete(self.run_parallel_async(*tasks))

    def test_estimate_fee(self):
        maker_fee = self.connector.estimate_fee_pct(True)
        self.assertAlmostEqual(maker_fee, Decimal("0.002"))
        taker_fee = self.connector.estimate_fee_pct(False)
        self.assertAlmostEqual(taker_fee, Decimal("0.002"))

    def order_response(self, fixture_data, nonce, side, trading_pair):
        self._t_nonce_mock.return_value = nonce
        order_id = f"{side.lower()}-{trading_pair}-{str(nonce)}"
        order_resp = fixture_data.copy()
        order_resp["clientOrderId"] = order_id
        return order_resp

    def _place_order(self, is_buy, amount, order_type, price, ex_order_id, get_order_fixture=None, get_trade_fixture=None) -> str:

        if API_MOCK_ENABLED:
            data = copy.deepcopy(fixture.CREATE_ORDER)
            data["data"]["id"] = ex_order_id
            self.web_app.update_response("post", BASE_API_URL, "/api/exchange/v1/new_order", data)

        if is_buy:
            cl_order_id = self.connector.buy(self.trading_pair, amount, order_type, price)
        else:
            cl_order_id = self.connector.sell(self.trading_pair, amount, order_type, price)

        if API_MOCK_ENABLED:
            if get_order_fixture is not None:
                data = copy.deepcopy(get_order_fixture)
                data['data'][0]['side'] = 'buy' if is_buy else 'sell'
                data['data'][0]['id'] = ex_order_id
                data['data'][0]['client_order_id'] = cl_order_id

                self.web_app.update_response("get", BASE_API_URL, "/api/exchange/v1/order", data, params={'order_id': ex_order_id})

            if get_trade_fixture is not None:
                # Increase trade id in order to new trade event was triggered by trade update loop
                global trade_id_inc
                trade_id_inc += 1

                data = copy.deepcopy(get_trade_fixture)
                trade_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                data['time'] = trade_time
                data['order_id'] = ex_order_id
                data['side'] = 'buy' if is_buy else 'sell'

                fixture.TRADE_HISTORY['data'].append(data)
                self.web_app.update_response("get", BASE_API_URL, "/api/exchange/v1/trade_history", fixture.TRADE_HISTORY)

        return cl_order_id

    def _cancel_order(self, cl_order_id, ex_order_id):
        self.connector.cancel(self.trading_pair, cl_order_id)
        if API_MOCK_ENABLED:
            resp = copy.deepcopy(fixture.GET_ORDER)
            resp["order_id"] = str(ex_order_id)
            resp["status"] = "cancelled"
            self.web_app.update_response("get", BASE_API_URL, "/api/exchange/v1/order", resp)

    def test_buy_and_sell(self):
        self.logger().info('Running test_buy_and_sell')

        price = self.connector.get_price(self.trading_pair, True) * Decimal("1.02")
        price = self.connector.quantize_order_price(self.trading_pair, price)
        amount = self.connector.quantize_order_amount(self.trading_pair, Decimal("5"))
        # quote_bal = self.connector.get_available_balance(self.quote_token)
        # base_bal = self.connector.get_available_balance(self.base_token)

        get_order = copy.deepcopy(fixture.GET_ORDER)
        get_order['data'][0]['status'] = 'filled'
        get_order['data'][0]['filled_quantity'] = '5'
        get_order['data'][0]['filled_cost'] = '2.68705'
        order_id = self._place_order(True, amount, OrderType.LIMIT, price, "1", get_order_fixture=get_order, get_trade_fixture=fixture.NEW_TRADE)
        order_completed_event = self.ev_loop.run_until_complete(self.event_logger.wait_for(BuyOrderCompletedEvent))
        self.ev_loop.run_until_complete(asyncio.sleep(10))

        trade_events = [t for t in self.event_logger.event_log if isinstance(t, OrderFilledEvent)]
        base_amount_traded = sum(t.amount for t in trade_events)
        quote_amount_traded = sum(t.amount * t.price for t in trade_events)

        self.assertTrue([evt.order_type == OrderType.LIMIT for evt in trade_events])
        self.assertEqual(order_id, order_completed_event.order_id)
        self.assertEqual(amount, order_completed_event.base_asset_amount)
        self.assertEqual("XRP", order_completed_event.base_asset)
        self.assertEqual("USDT", order_completed_event.quote_asset)
        self.assertAlmostEqual(base_amount_traded, order_completed_event.base_asset_amount)
        self.assertAlmostEqual(quote_amount_traded, order_completed_event.quote_asset_amount)
        # self.assertGreater(order_completed_event.fee_amount, Decimal(0))
        self.assertTrue(any([isinstance(event, BuyOrderCreatedEvent) and event.order_id == order_id
                             for event in self.event_logger.event_log]))

        # check available quote balance gets updated, we need to wait a bit for the balance message to arrive
        # Do not actually perform this check as Probit has rounding issues in balance calculations.
        # expected_quote_bal = quote_bal - quote_amount_traded
        # self.ev_loop.run_until_complete(asyncio.sleep(1))
        # self.assertAlmostEqual(expected_quote_bal, self.connector.get_available_balance(self.quote_token))

        # Reset the logs
        self.event_logger.clear()

        # Try to sell back the same amount to the exchange, and watch for completion event.
        price = self.connector.get_price(self.trading_pair, True) * Decimal("0.97")
        price = self.connector.quantize_order_price(self.trading_pair, price)
        amount = self.connector.quantize_order_amount(self.trading_pair, Decimal("5.0"))
        get_order = copy.deepcopy(fixture.GET_ORDER)
        get_order['data'][0]['status'] = 'filled'
        get_order['data'][0]['filled_quantity'] = '5'
        get_order['data'][0]['filled_cost'] = '2.68705'
        order_id = self._place_order(False, amount, OrderType.LIMIT, price, "2", get_order_fixture=get_order, get_trade_fixture=fixture.NEW_TRADE)
        order_completed_event = self.ev_loop.run_until_complete(self.event_logger.wait_for(SellOrderCompletedEvent))
        self.ev_loop.run_until_complete(asyncio.sleep(10))
        trade_events = [t for t in self.event_logger.event_log if isinstance(t, OrderFilledEvent)]
        print('TRADE EVENTS:', trade_events)
        base_amount_traded = sum(t.amount for t in trade_events)
        quote_amount_traded = sum(t.amount * t.price for t in trade_events)

        self.assertTrue([evt.order_type == OrderType.LIMIT for evt in trade_events])
        self.assertEqual(order_id, order_completed_event.order_id)
        self.assertEqual(amount, order_completed_event.base_asset_amount)
        self.assertEqual("XRP", order_completed_event.base_asset)
        self.assertEqual("USDT", order_completed_event.quote_asset)
        self.assertAlmostEqual(base_amount_traded, order_completed_event.base_asset_amount)
        self.assertAlmostEqual(quote_amount_traded, order_completed_event.quote_asset_amount)
        # self.assertGreater(order_completed_event.fee_amount, Decimal(0))
        self.assertTrue(any([isinstance(event, SellOrderCreatedEvent) and event.order_id == order_id
                             for event in self.event_logger.event_log]))

    def test_limit_makers_unfilled(self):
        # Probit.com does not support LIMIT_MAKER orders
        pass

    def test_limit_maker_rejections(self):
        # Probit.com does not support LIMIT_MAKER orders
        pass

    def test_cancel_all(self):
        self.logger().info('Running test_cancel_all')

        bid_price = self.connector.get_price(self.trading_pair, True)
        ask_price = self.connector.get_price(self.trading_pair, False)
        bid_price = self.connector.quantize_order_price(self.trading_pair, bid_price * Decimal("0.7"))
        ask_price = self.connector.quantize_order_price(self.trading_pair, ask_price * Decimal("1.5"))
        amount = self.connector.quantize_order_amount(self.trading_pair, Decimal("5"))

        get_order = copy.deepcopy(fixture.GET_ORDER)
        get_order['data'][0]['status'] = 'cancelled'
        buy_id = self._place_order(True, amount, OrderType.LIMIT, bid_price, "3", get_order_fixture=get_order)

        self.ev_loop.run_until_complete(asyncio.sleep(3))
        sell_id = self._place_order(False, amount, OrderType.LIMIT, ask_price, "4", get_order_fixture=get_order)

        self.ev_loop.run_until_complete(asyncio.sleep(1))
        asyncio.ensure_future(self.connector.cancel_all(20))
        self.ev_loop.run_until_complete(asyncio.sleep(3))
        cancel_events = [t for t in self.event_logger.event_log if isinstance(t, OrderCancelledEvent)]
        self.assertEqual({buy_id, sell_id}, {o.order_id for o in cancel_events})

    def test_order_price_precision(self):
        self.logger().info('Running test_order_price_precision')

        bid_price: Decimal = self.connector.get_price(self.trading_pair, True)
        ask_price: Decimal = self.connector.get_price(self.trading_pair, False)
        mid_price: Decimal = (bid_price + ask_price) / 2
        amount: Decimal = Decimal("10.000123456")

        # Make sure there's enough balance to make the limit orders.
        self.assertGreater(self.connector.get_balance("XRP"), Decimal("10"))
        self.assertGreater(self.connector.get_balance("USDT"), Decimal("10"))

        # Intentionally set some prices with too many decimal places s.t. they
        # need to be quantized. Also, place them far away from the mid-price s.t. they won't
        # get filled during the test.
        bid_price = mid_price * Decimal("0.9333192292111341")
        ask_price = mid_price * Decimal("1.0492431474884933")

        cl_order_id_1 = self._place_order(True, amount, OrderType.LIMIT, bid_price, "1", fixture.GET_ORDER)

        # Wait for the order created event and examine the order made
        self.ev_loop.run_until_complete(self.event_logger.wait_for(BuyOrderCreatedEvent))
        order = self.connector.in_flight_orders[cl_order_id_1]
        quantized_bid_price = self.connector.quantize_order_price(self.trading_pair, bid_price)
        quantized_bid_size = self.connector.quantize_order_amount(self.trading_pair, amount)
        self.assertEqual(quantized_bid_price, order.price)
        self.assertEqual(quantized_bid_size, order.amount)

        # Test ask order
        cl_order_id_2 = self._place_order(False, amount, OrderType.LIMIT, ask_price, "2", fixture.GET_ORDER)

        # Wait for the order created event and examine and order made
        self.ev_loop.run_until_complete(self.event_logger.wait_for(SellOrderCreatedEvent))
        order = self.connector.in_flight_orders[cl_order_id_2]
        quantized_ask_price = self.connector.quantize_order_price(self.trading_pair, Decimal(ask_price))
        quantized_ask_size = self.connector.quantize_order_amount(self.trading_pair, Decimal(amount))
        self.assertEqual(quantized_ask_price, order.price)
        self.assertEqual(quantized_ask_size, order.amount)

        asyncio.ensure_future(self.connector.cancel_all(20))
        self.ev_loop.run_until_complete(asyncio.sleep(3))

    def test_orders_saving_and_restoration(self):
        self.logger().info('Running test_orders_saving_and_restoration')

        config_path = "test_config"
        strategy_name = "test_strategy"
        sql = SQLConnectionManager(SQLConnectionType.TRADE_FILLS, db_path=self.db_path)
        order_id = None
        recorder = MarketsRecorder(sql, [self.connector], config_path, strategy_name)
        recorder.start()

        try:
            self.connector._in_flight_orders.clear()
            self.assertEqual(0, len(self.connector.tracking_states))

            # Try to put limit buy order for 0.02 ETH worth of ZRX, and watch for order creation event.
            current_bid_price: Decimal = self.connector.get_price(self.trading_pair, True)
            price: Decimal = current_bid_price * Decimal("0.8")
            price = self.connector.quantize_order_price(self.trading_pair, price)

            amount: Decimal = Decimal("5")
            amount = self.connector.quantize_order_amount(self.trading_pair, amount)
            get_order = copy.deepcopy(fixture.GET_ORDER)
            get_order['data'][0]['status'] = 'cancelled'
            cl_order_id = self._place_order(True, amount, OrderType.LIMIT, price, "5", get_order)
            order_created_event = self.ev_loop.run_until_complete(self.event_logger.wait_for(BuyOrderCreatedEvent))
            self.assertEqual(cl_order_id, order_created_event.order_id)

            # Verify tracking states
            self.assertEqual(1, len(self.connector.tracking_states))
            self.assertEqual(cl_order_id, list(self.connector.tracking_states.keys())[0])

            # Verify orders from recorder
            recorded_orders: List[Order] = recorder.get_orders_for_config_and_market(config_path, self.connector)
            self.assertEqual(1, len(recorded_orders))
            self.assertEqual(cl_order_id, recorded_orders[0].id)

            # Verify saved market states
            saved_market_states: MarketState = recorder.get_market_states(config_path, self.connector)
            self.assertIsNotNone(saved_market_states)
            self.assertIsInstance(saved_market_states.saved_state, dict)
            self.assertGreater(len(saved_market_states.saved_state), 0)

            # Close out the current market and start another market.
            self.connector.stop(self._clock)
            self.ev_loop.run_until_complete(asyncio.sleep(5))
            self.clock.remove_iterator(self.connector)
            for event_tag in self.events:
                self.connector.remove_listener(event_tag, self.event_logger)
            new_connector = ProbitMarket(API_KEY, API_SECRET, [self.trading_pair], True)
            for event_tag in self.events:
                new_connector.add_listener(event_tag, self.event_logger)
            recorder.stop()
            recorder = MarketsRecorder(sql, [new_connector], config_path, strategy_name)
            recorder.start()
            saved_market_states = recorder.get_market_states(config_path, new_connector)
            self.clock.add_iterator(new_connector)
            if not API_MOCK_ENABLED:
                self.ev_loop.run_until_complete(self.wait_til_ready(new_connector))
            self.assertEqual(0, len(new_connector.limit_orders))
            self.assertEqual(0, len(new_connector.tracking_states))
            new_connector.restore_tracking_states(saved_market_states.saved_state)
            self.assertEqual(1, len(new_connector.limit_orders))
            self.assertEqual(1, len(new_connector.tracking_states))

            # Cancel the order and verify that the change is saved.
            self._cancel_order(cl_order_id, "5")
            self.ev_loop.run_until_complete(self.event_logger.wait_for(OrderCancelledEvent))
            order_id = None
            self.assertEqual(0, len(new_connector.limit_orders))
            self.assertEqual(0, len(new_connector.tracking_states))
            saved_market_states = recorder.get_market_states(config_path, new_connector)
            self.assertEqual(0, len(saved_market_states.saved_state))
        finally:
            if order_id is not None:
                self.connector.cancel(self.trading_pair, cl_order_id)
                self.run_parallel(self.event_logger.wait_for(OrderCancelledEvent))

            recorder.stop()
            os.unlink(self.db_path)

    def test_update_last_prices(self):
        self.logger().info('Running test_update_last_prices')
        # This is basic test to see if order_book last_trade_price is initiated and updated.
        for order_book in self.connector.order_books.values():
            for _ in range(5):
                self.ev_loop.run_until_complete(asyncio.sleep(1))
                self.assertFalse(math.isnan(order_book.last_trade_price))

    def test_filled_orders_recorded(self):
        self.logger().info('Running test_filled_orders_recorded')

        config_path: str = "test_config"
        strategy_name: str = "test_strategy"
        sql = SQLConnectionManager(SQLConnectionType.TRADE_FILLS, db_path=self.db_path)
        order_id = None
        recorder = MarketsRecorder(sql, [self.connector], config_path, strategy_name)
        recorder.start()

        try:
            # Try to buy some token from the exchange, and watch for completion event.
            price = self.connector.get_price(self.trading_pair, True) * Decimal("1.05")
            price = self.connector.quantize_order_price(self.trading_pair, price)
            amount = self.connector.quantize_order_amount(self.trading_pair, Decimal("5.0"))
            get_order = copy.deepcopy(fixture.GET_ORDER)
            get_order['data'][0]['status'] = 'filled'
            order_id = self._place_order(True, amount, OrderType.LIMIT, price, "6", get_order_fixture=get_order,
                                         get_trade_fixture=fixture.NEW_TRADE)
            self.ev_loop.run_until_complete(self.event_logger.wait_for(BuyOrderCompletedEvent))
            self.ev_loop.run_until_complete(asyncio.sleep(10))

            # Reset the logs
            self.event_logger.clear()

            # Try to sell back the same amount to the exchange, and watch for completion event.
            price = self.connector.get_price(self.trading_pair, True) * Decimal("0.95")
            price = self.connector.quantize_order_price(self.trading_pair, price)
            amount = self.connector.quantize_order_amount(self.trading_pair, Decimal("5.0"))
            order_id = self._place_order(False, amount, OrderType.LIMIT, price, "7", get_order_fixture=get_order,
                                         get_trade_fixture=fixture.NEW_TRADE)
            self.ev_loop.run_until_complete(self.event_logger.wait_for(SellOrderCompletedEvent))
            self.ev_loop.run_until_complete(asyncio.sleep(10))
            # Query the persisted trade logs
            trade_fills: List[TradeFill] = recorder.get_trades_for_config(config_path)
            self.assertGreaterEqual(len(trade_fills), 2)
            buy_fills: List[TradeFill] = [t for t in trade_fills if t.trade_type == "BUY"]
            sell_fills: List[TradeFill] = [t for t in trade_fills if t.trade_type == "SELL"]
            self.assertGreaterEqual(len(buy_fills), 1)
            self.assertGreaterEqual(len(sell_fills), 1)

            order_id = None

        finally:
            if order_id is not None:
                self.connector.cancel(self.trading_pair, order_id)
                self.run_parallel(self.event_logger.wait_for(OrderCancelledEvent))

            recorder.stop()
            os.unlink(self.db_path)
