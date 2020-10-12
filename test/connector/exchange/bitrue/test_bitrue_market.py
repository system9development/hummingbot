from os.path import join, realpath
import sys; sys.path.insert(0, realpath(join(__file__, "../../../../../")))
import asyncio
import logging
from decimal import Decimal
import unittest
import contextlib
import time
import os
from typing import List
from unittest import mock
import conf
import math

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
from hummingbot.connector.exchange.bitrue.bitrue_market import BitrueMarket

from test.integration.humming_web_app import HummingWebApp
from . import fixture

logging.basicConfig(level=METRICS_LOG_LEVEL)
API_MOCK_ENABLED = conf.mock_api_enabled is not None and conf.mock_api_enabled.lower() in ['true', 'yes', '1']
API_KEY = "XXX" if API_MOCK_ENABLED else conf.bitrue_api_key
API_SECRET = "YYY" if API_MOCK_ENABLED else conf.bitrue_secret_key
BASE_API_URL = "www.bitrue.com"


class BitrueMarketUnitTest(unittest.TestCase):
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
    connector: BitrueMarket
    event_logger: EventLogger
    trading_pair = "XRP-USDT"
    base_token, quote_token = trading_pair.split("-")
    stack: contextlib.ExitStack

    @classmethod
    def setUpClass(cls):
        cls.ev_loop = asyncio.get_event_loop()

        if API_MOCK_ENABLED:
            cls.web_app = HummingWebApp.get_instance()
            cls.web_app.add_host_to_mock(BASE_API_URL, [])
            cls.web_app.start()
            cls.ev_loop.run_until_complete(cls.web_app.wait_til_started())
            cls._patcher = mock.patch("aiohttp.client.URL")
            cls._url_mock = cls._patcher.start()
            cls._url_mock.side_effect = cls.web_app.reroute_local
            cls.web_app.update_response("get", BASE_API_URL, "/api/v1/ticker/price", fixture.TICKER)
            cls.web_app.update_response("get", BASE_API_URL, "/api/v1/trades", fixture.TRADES)
            cls.web_app.update_response("get", BASE_API_URL, "/api/v1/myTrades", fixture.MY_TRADES)
            cls.web_app.update_response("get", BASE_API_URL, "/api/v1/ping", fixture.PING)
            cls.web_app.update_response("get", BASE_API_URL, "/api/v1/exchangeInfo", fixture.EXCHANGE_INFO)
            cls.web_app.update_response("get", BASE_API_URL, "/api/v1/depth", fixture.ORDER_BOOK)
            cls.web_app.update_response("get", BASE_API_URL, "/api/v1/account", fixture.ACCOUNT)
            cls.web_app.update_response("get", BASE_API_URL, "/api/v1/order", fixture.NEW_ORDER)
            cls.web_app.update_response("post", BASE_API_URL, "/api/v1/order", fixture.CREATE_ORDER)
            cls.web_app.update_response("delete", BASE_API_URL, "/api/v1/order", fixture.CANCEL_ORDER_RESPONSE)

        cls.clock: Clock = Clock(ClockMode.REALTIME)
        cls.connector: BitrueMarket = BitrueMarket(
            bitrue_api_key=API_KEY,
            bitrue_api_secret=API_SECRET,
            trading_pairs=[cls.trading_pair],
            trading_required=True
        )
        print("Initializing Bitrue market... this will take about a minute.")
        cls.clock.add_iterator(cls.connector)
        cls.stack: contextlib.ExitStack = contextlib.ExitStack()
        cls._clock = cls.stack.enter_context(cls.clock)

        cls.ev_loop.run_until_complete(cls.wait_til_ready())
        safe_ensure_future(cls.clock.run())
        print("Ready.")

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
        self.db_path: str = realpath(join(__file__, "../bitrue_connector_test.sqlite"))
        # Trade id increment param required for new trades tracking
        self.trade_id_inc = 1
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
        self.assertAlmostEqual(maker_fee, Decimal("0.00098"))
        taker_fee = self.connector.estimate_fee_pct(False)
        self.assertAlmostEqual(taker_fee, Decimal("0.00098"))

    def order_response(self, fixture_data, nonce, side, trading_pair):
        self._t_nonce_mock.return_value = nonce
        order_id = f"{side.lower()}-{trading_pair}-{str(nonce)}"
        order_resp = fixture_data.copy()
        order_resp["clientOrderId"] = order_id
        return order_resp

    def _place_order(self, is_buy, amount, order_type, price, ex_order_id, get_order_fixture=None, get_trade_fixture=None) -> str:
        if API_MOCK_ENABLED:
            data = fixture.CREATE_ORDER.copy()
            data["orderId"] = ex_order_id
            self.web_app.update_response("post", BASE_API_URL, "/api/v1/order", data)
        if is_buy:
            cl_order_id = self.connector.buy(self.trading_pair, amount, order_type, price)
        else:
            cl_order_id = self.connector.sell(self.trading_pair, amount, order_type, price)
        if API_MOCK_ENABLED:
            if get_order_fixture is not None:
                data = get_order_fixture.copy()
                data['side'] = 'BUY' if is_buy else 'SELL'
                data['orderId'] = str(ex_order_id)
                self.web_app.update_response("get", BASE_API_URL, "/api/v1/order", data, params={'orderId': data['orderId']})
            if get_trade_fixture is not None:
                data = get_trade_fixture.copy()
                # Increase trade id in order to new trade event was triggered by trade update loop
                self.trade_id_inc += 1
                data['id'] += self.trade_id_inc
                data['orderId'] = str(ex_order_id)
                data['isBuyer'] = True if is_buy else False
                fixture.MY_TRADES.append(data)
                self.web_app.update_response("get", BASE_API_URL, "/api/v1/myTrades", fixture.MY_TRADES)
        return cl_order_id

    def _cancel_order(self, cl_order_id, ex_order_id):
        self.connector.cancel(self.trading_pair, cl_order_id)
        if API_MOCK_ENABLED:
            resp = fixture.CANCELED_ORDER.copy()
            resp["orderId"] = str(ex_order_id)
            self.web_app.update_response("get", BASE_API_URL, "/api/v1/order", resp,
                                         params={'orderId': resp["orderId"]})

    def utest_buy_and_sell(self):
        price = self.connector.get_price(self.trading_pair, True) * Decimal("1.05")
        price = self.connector.quantize_order_price(self.trading_pair, price)
        amount = self.connector.quantize_order_amount(self.trading_pair, Decimal("10"))
        quote_bal = self.connector.get_available_balance(self.quote_token)
        base_bal = self.connector.get_available_balance(self.base_token)

        order_id = self._place_order(True, amount, OrderType.LIMIT, price, 1, get_order_fixture=fixture.FILLED_ORDER, get_trade_fixture=fixture.NEW_TRADE)
        order_completed_event = self.ev_loop.run_until_complete(self.event_logger.wait_for(BuyOrderCompletedEvent))
        self.ev_loop.run_until_complete(asyncio.sleep(5))

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
        expected_quote_bal = quote_bal - quote_amount_traded
        self.ev_loop.run_until_complete(asyncio.sleep(1))
        self.assertAlmostEqual(expected_quote_bal, self.connector.get_available_balance(self.quote_token))

        # Reset the logs
        self.event_logger.clear()

        # Try to sell back the same amount to the exchange, and watch for completion event.
        price = self.connector.get_price(self.trading_pair, True) * Decimal("0.95")
        price = self.connector.quantize_order_price(self.trading_pair, price)
        amount = self.connector.quantize_order_amount(self.trading_pair, Decimal("10.0"))
        order_id = self._place_order(False, amount, OrderType.LIMIT, price, 2, get_order_fixture=fixture.FILLED_ORDER, get_trade_fixture=fixture.NEW_TRADE)
        order_completed_event = self.ev_loop.run_until_complete(self.event_logger.wait_for(SellOrderCompletedEvent))
        self.ev_loop.run_until_complete(asyncio.sleep(5))
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
        self.assertTrue(any([isinstance(event, SellOrderCreatedEvent) and event.order_id == order_id
                             for event in self.event_logger.event_log]))

        # check available base balance gets updated, we need to wait a bit for the balance message to arrive
        expected_base_bal = base_bal

        self.ev_loop.run_until_complete(asyncio.sleep(1))
        self.assertAlmostEqual(expected_base_bal, self.connector.get_available_balance(self.base_token), 5)

    def test_limit_makers_unfilled(self):
        # Bitrue.com does not support LIMIT_MAKER orders
        pass

    def test_limit_maker_rejections(self):
        # Bitrue.com does not support LIMIT_MAKER orders
        pass

    def utest_cancel_all(self):
        bid_price = self.connector.get_price(self.trading_pair, True)
        ask_price = self.connector.get_price(self.trading_pair, False)
        bid_price = self.connector.quantize_order_price(self.trading_pair, bid_price * Decimal("0.7"))
        ask_price = self.connector.quantize_order_price(self.trading_pair, ask_price * Decimal("1.5"))
        amount = self.connector.quantize_order_amount(self.trading_pair, Decimal("10"))

        buy_id = self._place_order(True, amount, OrderType.LIMIT, bid_price, 1, get_order_fixture=fixture.NEW_ORDER)
        self.ev_loop.run_until_complete(asyncio.sleep(3))
        sell_id = self._place_order(False, amount, OrderType.LIMIT, ask_price, 2, get_order_fixture=fixture.NEW_ORDER)

        if API_MOCK_ENABLED:
            resp = fixture.CANCELED_ORDER.copy()
            resp["side"] = "BUY"
            resp["orderId"] = "1"
            self.web_app.update_response("get", BASE_API_URL, "/api/v1/order", resp,
                                         params={'orderId': '1'})
            resp = fixture.CANCELED_ORDER.copy()
            resp["side"] = "SELL"
            resp["orderId"] = "2"
            self.web_app.update_response("get", BASE_API_URL, "/api/v1/order", resp,
                                         params={'orderId': '2'})

        self.ev_loop.run_until_complete(asyncio.sleep(1))
        asyncio.ensure_future(self.connector.cancel_all(3))

        self.ev_loop.run_until_complete(asyncio.sleep(3))
        cancel_events = [t for t in self.event_logger.event_log if isinstance(t, OrderCancelledEvent)]
        self.assertEqual({buy_id, sell_id}, {o.order_id for o in cancel_events})

    def test_order_price_precision(self):
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

        cl_order_id_1 = self._place_order(True, amount, OrderType.LIMIT, bid_price, 1, fixture.NEW_ORDER)

        # Wait for the order created event and examine the order made
        self.ev_loop.run_until_complete(self.event_logger.wait_for(BuyOrderCreatedEvent))
        order = self.connector.in_flight_orders[cl_order_id_1]
        quantized_bid_price = self.connector.quantize_order_price(self.trading_pair, bid_price)
        quantized_bid_size = self.connector.quantize_order_amount(self.trading_pair, amount)
        self.assertEqual(quantized_bid_price, order.price)
        self.assertEqual(quantized_bid_size, order.amount)

        # Test ask order
        cl_order_id_2 = self._place_order(False, amount, OrderType.LIMIT, ask_price, 2, fixture.NEW_ORDER)

        # Wait for the order created event and examine and order made
        self.ev_loop.run_until_complete(self.event_logger.wait_for(SellOrderCreatedEvent))
        order = self.connector.in_flight_orders[cl_order_id_2]
        quantized_ask_price = self.connector.quantize_order_price(self.trading_pair, Decimal(ask_price))
        quantized_ask_size = self.connector.quantize_order_amount(self.trading_pair, Decimal(amount))
        self.assertEqual(quantized_ask_price, order.price)
        self.assertEqual(quantized_ask_size, order.amount)

        self._cancel_order(cl_order_id_1, 1)
        self._cancel_order(cl_order_id_2, 2)

    def test_orders_saving_and_restoration(self):
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

            amount: Decimal = Decimal("10")
            amount = self.connector.quantize_order_amount(self.trading_pair, amount)

            cl_order_id = self._place_order(True, amount, OrderType.LIMIT, price, 1, fixture.NEW_ORDER)
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
            new_connector = BitrueMarket(API_KEY, API_SECRET, [self.trading_pair], True)
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
            self._cancel_order(cl_order_id, 1)
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
        # This is basic test to see if order_book last_trade_price is initiated and updated.
        for order_book in self.connector.order_books.values():
            for _ in range(5):
                self.ev_loop.run_until_complete(asyncio.sleep(1))
                print(order_book.last_trade_price)
                self.assertFalse(math.isnan(order_book.last_trade_price))

    def utest_filled_orders_recorded(self):
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
            amount = self.connector.quantize_order_amount(self.trading_pair, Decimal("10.0"))

            order_id = self._place_order(True, amount, OrderType.LIMIT, price, 1, get_order_fixture=fixture.FILLED_ORDER,
                                         get_trade_fixture=fixture.NEW_TRADE)
            self.ev_loop.run_until_complete(self.event_logger.wait_for(BuyOrderCompletedEvent))
            self.ev_loop.run_until_complete(asyncio.sleep(10))

            # Reset the logs
            self.event_logger.clear()

            # Try to sell back the same amount to the exchange, and watch for completion event.
            price = self.connector.get_price(self.trading_pair, True) * Decimal("0.95")
            price = self.connector.quantize_order_price(self.trading_pair, price)
            amount = self.connector.quantize_order_amount(self.trading_pair, Decimal("10.0"))
            order_id = self._place_order(False, amount, OrderType.LIMIT, price, 2, get_order_fixture=fixture.FILLED_ORDER,
                                         get_trade_fixture=fixture.NEW_TRADE)
            self.ev_loop.run_until_complete(self.event_logger.wait_for(SellOrderCompletedEvent))
            self.ev_loop.run_until_complete(asyncio.sleep(5))
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
