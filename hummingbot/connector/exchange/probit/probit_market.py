import logging
from typing import (
    Dict,
    List,
    Optional,
    Any,
    # AsyncIterable
)
from decimal import Decimal
import asyncio
import math
import time
import copy
from async_timeout import timeout
from datetime import datetime

from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.logger import HummingbotLogger
from hummingbot.core.clock import Clock
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.event.events import (
    MarketEvent,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent,
    OrderFilledEvent,
    OrderCancelledEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderType,
    TradeType,
    TradeFee
)
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.exchange.probit.probit_order_book_tracker import ProbitOrderBookTracker
from hummingbot.connector.exchange.probit.probit_user_stream_tracker import ProbitUserStreamTracker
from hummingbot.connector.exchange.probit.probit_auth import ProbitAuth
from hummingbot.connector.exchange.probit.probit_in_flight_order import ProbitInFlightOrder
from hummingbot.connector.exchange.probit import probit_utils
from hummingbot.connector.exchange.probit.probit_api_client import ProbitAPIClient

s_decimal_NaN = Decimal("nan")


class ProbitMarket(ExchangeBase):
    """
    ProbitMarket connects with Probit.com exchange and provides order book pricing, user account tracking and
    trading functionality.
    """
    API_CALL_TIMEOUT = 10.0
    SHORT_POLL_INTERVAL = 3.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 3.0
    UPDATE_TRADES_MIN_INTERVAL = 3.0
    LONG_POLL_INTERVAL = 5.0
    ERROR_TIMEOUT = 5.0

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self,
                 probit_api_key: str,
                 probit_api_secret: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True
                 ):
        """
        :param probit_api_key: The API key to connect to private Probit.com APIs.
        :param probit_api_secret: The API secret.
        :param trading_pairs: The market trading pairs which to track order book data.
        :param trading_required: Whether actual trading is needed.
        """
        super().__init__()
        self._trading_required = trading_required
        self._probit_auth = ProbitAuth(probit_api_key, probit_api_secret)
        self._trading_pairs = trading_pairs
        self._order_book_tracker = ProbitOrderBookTracker(trading_pairs=trading_pairs)
        self._user_stream_tracker = ProbitUserStreamTracker(self._probit_auth, trading_pairs)

        self._ev_loop = asyncio.get_event_loop()
        self._poll_notifier = asyncio.Event()

        self._last_timestamp = 0
        self._last_poll_timestamp = 0
        self._current_timestamp = 0

        self._in_flight_orders = {}  # Dict[client_order_id:str, ProbitInFlightOrder]
        self._order_not_found_records = {}  # Dict[client_order_id:str, count:int]

        self._trading_rules = {}  # Dict[trading_pair:str, TradingRule]
        self._status_polling_task = None
        self._user_stream_event_listener_task = None
        self._trading_rules_polling_task = None
        self._max_trade_ts_task = None
        self._last_poll_timestamp = 0

        self._real_time_balance_update = False

        self.last_max_trade_ts: Dict[str, datetime] = {}

        self.probit_client = ProbitAPIClient(probit_api_key, probit_api_secret)

    @property
    def name(self) -> str:
        return "probit"

    @property
    def current_timestamp(self) -> float:
        return self._current_timestamp

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    @property
    def trading_rules(self) -> Dict[str, TradingRule]:
        return self._trading_rules

    @property
    def in_flight_orders(self) -> Dict[str, ProbitInFlightOrder]:
        return self._in_flight_orders

    @property
    def status_dict(self) -> Dict[str, bool]:
        """
        A dictionary of statuses of various connector's components.
        """
        return {
            "order_books_initialized": self._order_book_tracker.ready,
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0,
            "user_stream_initialized":
                self._user_stream_tracker.data_source.last_recv_time > 0 if self._trading_required else True,
            "max_trade_ts_initialized": len(self.last_max_trade_ts) > 0
        }

    @property
    def ready(self) -> bool:
        """
        :return True when all statuses pass, this might take 5-10 seconds for all the connector's components and
        services to be ready.
        """
        return all(self.status_dict.values())

    @property
    def limit_orders(self) -> List[LimitOrder]:
        return [
            in_flight_order.to_limit_order()
            for in_flight_order in self._in_flight_orders.values()
        ]

    @property
    def tracking_states(self) -> Dict[str, any]:
        """
        :return active in-flight orders in json format, is used to save in sqlite db.
        """
        return {
            key: value.to_json()
            for key, value in self._in_flight_orders.items()
            if not value.is_done
        }

    def restore_tracking_states(self, saved_states: Dict[str, any]):
        """
        Restore in-flight orders from saved tracking states, this is st the connector can pick up on where it left off
        when it disconnects.
        :param saved_states: The saved tracking_states.
        """
        self._in_flight_orders.update({
            key: ProbitInFlightOrder.from_json(value)
            for key, value in saved_states.items()
        })

    def supported_order_types(self) -> List[OrderType]:
        """
        :return a list of OrderType supported by this connector.
        """
        return [OrderType.LIMIT]

    def start(self, clock: Clock, timestamp: float):
        """
        This function is called automatically by the clock.
        """
        super().start(clock, timestamp)

    def stop(self, clock: Clock):
        """
        This function is called automatically by the clock.
        """
        super().stop(clock)

    async def start_network(self):
        """
        This function is required by NetworkIterator base class and is called automatically.
        It starts tracking order book, polling trading rules,
        updating statuses and tracking user data.
        """
        self._order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())
            self._max_trade_ts_task = safe_ensure_future(self._update_max_trade_ts())

    async def stop_network(self):
        """
        This function is required by NetworkIterator base class and is called automatically.
        """
        self._order_book_tracker.stop()
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
            self._status_polling_task = None
        if self._trading_rules_polling_task is not None:
            self._trading_rules_polling_task.cancel()
            self._trading_rules_polling_task = None
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
            self._status_polling_task = None
        if self._user_stream_tracker_task is not None:
            self._user_stream_tracker_task.cancel()
            self._user_stream_tracker_task = None
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()
            self._user_stream_event_listener_task = None

    async def check_network(self) -> NetworkStatus:
        """
        This function is required by NetworkIterator base class and is called periodically to check
        the network connection. Simply ping the network (or call any light weight public API).
        """
        try:
            await self.probit_client.ping()
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    async def _update_max_trade_ts(self):
        """
        Initialize self.last_max_trade_ts dict in order to track new trades using REST API
        """

        if len(self.last_max_trade_ts) == 0:
            try:
                for trading_pair in self._trading_pairs:
                    self.last_max_trade_ts[trading_pair] = datetime(1970, 1, 1)
                    trades: List[Dict[str, any]] = await self.probit_client.get_trade_history(symbol=probit_utils.convert_to_exchange_trading_pair(trading_pair), start_time=datetime(1970, 1, 1), end_time=datetime.utcnow(), limit=2)
                    max_trade_ts = datetime(1970, 1, 1)
                    for trade in trades[::-1]:
                        # Process only trades that haven't been seen before
                        trade_time = datetime.strptime(trade['time'], '%Y-%m-%dT%H:%M:%S.%fZ')
                        if trade_time > self.last_max_trade_ts[trading_pair]:
                            # Update last_max_trade_ts
                            max_trade_ts = max(max_trade_ts, trade_time)
                    self.last_max_trade_ts[trading_pair] = max(max_trade_ts, self.last_max_trade_ts[trading_pair])
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with REST API request...",
                                    exc_info=True)
                await asyncio.sleep(self.ERROR_TIMEOUT)

    async def _trading_rules_polling_loop(self):
        """
        Periodically update trading rule.
        """
        while True:
            try:
                await self._update_trading_rules()
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(e)
                self.logger().network(f"Unexpected error while fetching trading rules. Error: {str(e)}",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch new trading rules from Probit.com. "
                                                      "Check network connection.")
                await asyncio.sleep(0.5)

    async def _update_trading_rules(self):
        markets_info = await self.probit_client.get_markets()
        self._trading_rules.clear()
        self._trading_rules = self._format_trading_rules(markets_info)

    def _format_trading_rules(self, markets_info: List[Dict[str, Any]]) -> Dict[str, TradingRule]:
        """
        Converts json API response into a dictionary of trading rules.
        :param instruments_info: The json API response
        :return A dictionary of trading rules.
        Response Example:
        [{'id': 'EOS-USDT', 'base_currency_id': 'EOS', 'quote_currency_id': 'USDT',
            'min_price': '0.0001', 'max_price': '9999999999999999', 'price_increment': '0.0001',
            'min_quantity': '0.01', 'max_quantity': '9999999999999999', 'quantity_precision': 2,
            'min_cost': '1', 'max_cost': '9999999999999999', 'cost_precision': 8,
            'taker_fee_rate': '0.2', 'maker_fee_rate': '0.2', 'show_in_ui': True, 'closed': False}]
        """
        result = {}
        for market in markets_info:
            try:
                trading_pair = probit_utils.convert_from_exchange_trading_pair(market['id'])
                supports_limit_orders = True
                supports_market_orders = True
                max_qty = market['max_quantity']
                min_qty = market['min_quantity']
                price_step = market['price_increment']
                quantity_precision = market['quantity_precision']

                quantity_step = Decimal("1") / Decimal(str(math.pow(10, quantity_precision)))

                result[trading_pair] = TradingRule(trading_pair=trading_pair,
                                                   min_order_size=Decimal(min_qty),
                                                   max_order_size=Decimal(max_qty),
                                                   supports_limit_orders = supports_limit_orders,
                                                   supports_market_orders = supports_market_orders,
                                                   min_price_increment=Decimal(price_step),
                                                   min_base_amount_increment=Decimal(quantity_step)
                                                   )
            except Exception:
                self.logger().error(f"Error parsing the trading pair rule: {market}.\nSkipping.", exc_info=True)
        return result

    def get_order_price_quantum(self, trading_pair: str, price: Decimal):
        """
        Returns a price step, a minimum price increment for a given trading pair.
        """
        trading_rule = self._trading_rules[trading_pair]
        return trading_rule.min_price_increment

    def get_order_size_quantum(self, trading_pair: str, order_size: Decimal):
        """
        Returns an order amount step, a minimum amount increment for a given trading pair.
        """
        trading_rule = self._trading_rules[trading_pair]
        return Decimal(trading_rule.min_base_amount_increment)

    def get_order_book(self, trading_pair: str) -> OrderBook:
        if trading_pair not in self._order_book_tracker.order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return self._order_book_tracker.order_books[trading_pair]

    def buy(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
            price: Decimal = s_decimal_NaN, **kwargs) -> str:
        """
        Buys an amount of base asset (of the given trading pair). This function returns immediately.
        To see an actual order, you'll have to wait for BuyOrderCreatedEvent.
        :param trading_pair: The market (e.g. BTC-USDT) to buy from
        :param amount: The amount in base token value
        :param order_type: The order type
        :param price: The price (note: this is no longer optional)
        :returns A new internal order id
        """
        order_id: str = probit_utils.get_new_client_order_id('buy', trading_pair)
        safe_ensure_future(self._create_order(TradeType.BUY, order_id, trading_pair, amount, order_type, price))
        return order_id

    def sell(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
             price: Decimal = s_decimal_NaN, **kwargs) -> str:
        """
        Sells an amount of base asset (of the given trading pair). This function returns immediately.
        To see an actual order, you'll have to wait for SellOrderCreatedEvent.
        :param trading_pair: The market (e.g. BTC-USDT) to sell from
        :param amount: The amount in base token value
        :param order_type: The order type
        :param price: The price (note: this is no longer optional)
        :returns A new internal order id
        """
        order_id: str = probit_utils.get_new_client_order_id('sell', trading_pair)
        safe_ensure_future(self._create_order(TradeType.SELL, order_id, trading_pair, amount, order_type, price))
        return order_id

    def cancel(self, trading_pair: str, order_id: str):
        """
        Cancel an order. This function returns immediately.
        To get the cancellation result, you'll have to wait for OrderCancelledEvent.
        :param trading_pair: The market (e.g. BTC-USDT) of the order.
        :param order_id: The internal order id (also called client_order_id)
        """
        safe_ensure_future(self._execute_cancel(trading_pair, order_id))
        return order_id

    async def _create_order(self,
                            trade_type: TradeType,
                            order_id: str,
                            trading_pair: str,
                            amount: Decimal,
                            order_type: OrderType,
                            price: Decimal):
        """
        Calls create-order API end point to place an order, starts tracking the order and triggers order created event.
        :param trade_type: buy or sell
        :param order_id: Internal order id (also called client_order_id)
        :param trading_pair: The market to place order
        :param amount: The order amount (in base token value)
        :param order_type: The order type
        :param price: The order price
        """

        trading_rule = self._trading_rules[trading_pair]

        amount = self.quantize_order_amount(trading_pair, amount)
        price = self.quantize_order_price(trading_pair, price)

        trade_type_exch = probit_utils.get_probit_trade_type(trade_type)
        order_type_exch = probit_utils.get_probit_order_type(order_type)
        if amount < trading_rule.min_order_size:
            raise ValueError(f"Buy order amount {amount} is lower than the minimum order size "
                             f"{trading_rule.min_order_size}.")

        try:
            order_result = await self.probit_client.create_order(
                client_order_id=order_id,
                symbol=probit_utils.convert_to_exchange_trading_pair(trading_pair),
                side=trade_type_exch,
                type=order_type_exch,
                quantity=amount,
                price=price)

            exchange_order_id = str(order_result["id"])
            self.start_tracking_order(order_id,
                                      exchange_order_id,
                                      trading_pair,
                                      trade_type,
                                      price,
                                      amount,
                                      order_type
                                      )

            self.logger().info(f"Created {order_type.name} {trade_type.name} order {order_id} for "
                               f"{amount} {trading_pair}.")

            event_tag = MarketEvent.BuyOrderCreated if trade_type is TradeType.BUY else MarketEvent.SellOrderCreated
            event_class = BuyOrderCreatedEvent if trade_type is TradeType.BUY else SellOrderCreatedEvent
            self.trigger_event(event_tag,
                               event_class(
                                   self.current_timestamp,
                                   order_type,
                                   trading_pair,
                                   amount,
                                   price,
                                   order_id
                               ))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.stop_tracking_order(order_id)
            self.logger().network(
                f"Error submitting {trade_type.name} {order_type.name} order to Probit.com for "
                f"{amount} {trading_pair} "
                f"{price}.",
                exc_info=True,
                app_warning_msg=str(e)
            )
            self.trigger_event(MarketEvent.OrderFailure,
                               MarketOrderFailureEvent(self.current_timestamp, order_id, order_type))

    def start_tracking_order(self,
                             order_id: str,
                             exchange_order_id: str,
                             trading_pair: str,
                             trade_type: TradeType,
                             price: Decimal,
                             amount: Decimal,
                             order_type: OrderType):
        """
        Starts tracking an order by simply adding it into _in_flight_orders dictionary.
        """
        self._in_flight_orders[order_id] = ProbitInFlightOrder(
            client_order_id=order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=trading_pair,
            order_type=order_type,
            trade_type=trade_type,
            price=price,
            amount=amount
        )

    def stop_tracking_order(self, order_id: str):
        """
        Stops tracking an order by simply removing it from _in_flight_orders dictionary.
        """
        if order_id in self._in_flight_orders:
            del self._in_flight_orders[order_id]

    async def _execute_cancel(self, trading_pair: str, order_id: str, wait_for_status: bool = False) -> str:
        """
        Executes order cancellation process by first calling cancel-order API. The API result doesn't confirm whether
        the cancellation is successful, it simply states it receives the request.
        :param trading_pair: The market trading pair
        :param order_id: The internal order id
        :param wait_for_status: Whether to wait for the cancellation result, this is done by waiting for
        order.last_state to change to CANCELED
        """
        try:
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is None:
                raise ValueError(f"Failed to cancel order - {order_id}. Order not found.")
            if tracked_order.exchange_order_id is None:
                await tracked_order.get_exchange_order_id()
            ex_order_id = tracked_order.exchange_order_id

            result = await self.probit_client.cancel_order(symbol=probit_utils.convert_to_exchange_trading_pair(trading_pair), order_id=ex_order_id)
            if isinstance(result, dict) and str(result.get("id")) == ex_order_id:
                self.logger().info(f"Successfully cancelled order {order_id}.")
                self.trigger_event(
                    MarketEvent.OrderCancelled,
                    OrderCancelledEvent(
                        self.current_timestamp,
                        order_id))

                if wait_for_status:
                    from hummingbot.core.utils.async_utils import wait_til
                    await wait_til(lambda: tracked_order.is_cancelled)
                self.stop_tracking_order(order_id)
                return order_id

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().network(
                f"Failed to cancel order {order_id}: {str(e)}",
                exc_info=True,
                app_warning_msg=f"Failed to cancel the order {order_id} on Probit. "
                                f"Check API key and network connection."
            )

    async def _status_polling_loop(self):
        """
        Periodically update user balances and order status via REST API. This serves as a fallback measure for web
        socket API updates.
        """
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()
                await safe_gather(
                    self._update_balances(),
                    self._update_order_status(),
                    self._update_trades()
                )
                self._last_poll_timestamp = self._current_timestamp
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(str(e), exc_info=True)
                self.logger().network("Unexpected error while fetching account updates.",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch account updates from Probit.com. "
                                                      "Check API key and network connection.")
                await asyncio.sleep(0.5)

    async def _update_balances(self):
        """
        Calls REST API to update total and available balances.
        """
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        balances = await self.probit_client.get_balance()
        for balance_entry in balances:
            asset_name = balance_entry["currency_id"].upper()
            free_balance = Decimal(balance_entry["available"])
            total_balance = Decimal(balance_entry["total"])
            self._account_available_balances[asset_name] = free_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

        # Take inflight orders snapshot
        self._in_flight_orders_snapshot = {k: copy.copy(v) for k, v in self._in_flight_orders.items()}
        self._in_flight_orders_snapshot_timestamp = self._current_timestamp

    async def _update_order_status(self):
        """
        Calls REST API to get status update for each in-flight order.
        """
        last_tick = self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL
        current_tick = self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL

        if current_tick > last_tick and len(self._in_flight_orders) > 0:
            tracked_orders = list(self._in_flight_orders.values())
            tasks = list()
            for tracked_order in tracked_orders:
                order_id: str = tracked_order.get_exchange_order_id()
                tasks.append(self.probit_client.get_order(symbol=probit_utils.convert_to_exchange_trading_pair(tracked_order.trading_pair), order_id=order_id))

            self.logger().debug(f"Polling for order status updates of {len(tasks)} orders.")
            exch_orders = await safe_gather(*tasks, return_exceptions=True)
            for exch_order in exch_orders:
                if isinstance(exch_order, Exception):
                    raise exch_order
                if "id" not in exch_order[0]:
                    self.logger().info(f"'id' param is not in response: {exch_order}")
                    continue
                self._process_order_message(exch_order[0])

    async def _update_trades(self):
        """
        Calls REST API to fetch recent trades.
        """
        last_tick = self._last_poll_timestamp / self.UPDATE_TRADES_MIN_INTERVAL
        current_tick = self.current_timestamp / self.UPDATE_TRADES_MIN_INTERVAL

        if current_tick > last_tick and len(self._in_flight_orders) > 0:
            tasks = []
            try:
                for trading_pair in self._trading_pairs:
                    end_time: datetime = datetime.utcnow()
                    tasks.append(self.probit_client.get_trade_history(probit_utils.convert_to_exchange_trading_pair(trading_pair), start_time=self.last_max_trade_ts[trading_pair], end_time=end_time, limit=1000))

                self.logger().debug(f"Polling for recent trades updates of {len(tasks)} orders.")
                results = await safe_gather(*tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        raise result
                    trades: List[Dict[str, any]] = result

                    max_trade_ts = datetime(1970, 1, 1)
                    for trade in trades[::-1]:
                        trading_pair = probit_utils.convert_from_exchange_trading_pair(trade['market_id'])
                        # Process only trades that haven't been seen before
                        trade_time = datetime.strptime(trade['time'], '%Y-%m-%dT%H:%M:%S.%fZ')
                        if trade_time > self.last_max_trade_ts[trading_pair] and trade['status'] == 'settled':
                            self._process_trade_message(trade)
                            # Update last_max_trade_ts
                            max_trade_ts = max(max_trade_ts, trade_time)
                    self.last_max_trade_ts[trading_pair] = max(max_trade_ts, self.last_max_trade_ts[trading_pair])
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with REST API request...",
                                    exc_info=True)
                await asyncio.sleep(self.ERROR_TIMEOUT)

    def _process_order_message(self, order_msg: Dict[str, Any]):
        """
        Updates in-flight order and triggers cancellation or failure event if needed.
        :param order_msg: The order response from either REST or web socket API (they are of the same format)
        """
        client_order_id = order_msg["client_order_id"]
        if client_order_id not in self._in_flight_orders:
            return
        tracked_order = self._in_flight_orders[client_order_id]
        # Update order execution status
        if tracked_order.last_state != order_msg["status"]:

            tracked_order.last_state = order_msg["status"]
            # If status changed to 'FILLED', trigger order completion event
            if order_msg["status"] == 'filled':
                event_tag = MarketEvent.BuyOrderCompleted if tracked_order.trade_type is TradeType.BUY \
                    else MarketEvent.SellOrderCompleted
                event_class = BuyOrderCompletedEvent if tracked_order.trade_type is TradeType.BUY \
                    else SellOrderCompletedEvent
                self.trigger_event(event_tag,
                                   event_class(self.current_timestamp,
                                               tracked_order.client_order_id,
                                               tracked_order.base_asset,
                                               tracked_order.quote_asset,
                                               tracked_order.fee_asset,
                                               Decimal(order_msg["filled_quantity"]),
                                               Decimal(order_msg["filled_cost"]),
                                               tracked_order.fee_paid,
                                               tracked_order.order_type))

        if tracked_order.is_cancelled:
            self.logger().info(f"Successfully cancelled order {client_order_id}.")
            self.trigger_event(MarketEvent.OrderCancelled,
                               OrderCancelledEvent(
                                   self.current_timestamp,
                                   client_order_id))
            tracked_order.cancelled_event.set()
            self.stop_tracking_order(client_order_id)
        elif tracked_order.is_failure:
            self.logger().info(f"The market order {client_order_id} has failed according to order status API.")
            self.trigger_event(MarketEvent.OrderFailure,
                               MarketOrderFailureEvent(
                                   self.current_timestamp,
                                   client_order_id,
                                   tracked_order.order_type
                               ))
            self.stop_tracking_order(client_order_id)

    def _process_trade_message(self, trade_msg: Dict[str, Any]):
        """
        Updates in-flight order and trigger order filled event for trade message received. Triggers order completed
        event if the total executed amount equals to the specified order amount.
        """
        track_order = [o for o in self._in_flight_orders.values() if str(trade_msg["order_id"]) == o.exchange_order_id]
        if not track_order:
            return

        tracked_order = track_order[0]
        updated = tracked_order.update_with_trade_update(trade_msg)
        if not updated:
            return
        self.trigger_event(
            MarketEvent.OrderFilled,
            OrderFilledEvent(
                self.current_timestamp,
                tracked_order.client_order_id,
                tracked_order.trading_pair,
                tracked_order.trade_type,
                tracked_order.order_type,
                Decimal(str(trade_msg["price"])),
                Decimal(str(trade_msg["quantity"])),
                TradeFee(0.0, [(trade_msg["fee_currency_id"], Decimal(trade_msg["fee_amount"]))]),
                exchange_trade_id=str(trade_msg["id"])
            )
        )

        if tracked_order.last_state == 'filled':
            if tracked_order.executed_amount_base == tracked_order.amount:
                self.stop_tracking_order(tracked_order.client_order_id)

    async def cancel_all(self, timeout_seconds: float):
        """
        Cancels all in-flight orders and waits for cancellation results.
        Used by bot's top level stop and exit commands (cancelling outstanding orders on exit)
        :param timeout_seconds: The timeout at which the operation will be canceled.
        :returns List of CancellationResult which indicates whether each order is successfully cancelled.
        """
        incomplete_orders = [o for o in self._in_flight_orders.values() if not o.is_done]
        tasks = [self._execute_cancel(o.trading_pair, o.client_order_id, True) for o in incomplete_orders]
        order_id_set = set([o.client_order_id for o in incomplete_orders])
        successful_cancellations = []
        try:
            async with timeout(timeout_seconds):
                results = await safe_gather(*tasks, return_exceptions=True)
                for result in results:
                    if result is not None and not isinstance(result, Exception):
                        order_id_set.remove(result)
                        successful_cancellations.append(CancellationResult(result, True))
        except Exception:
            self.logger().error("Cancel all failed.", exc_info=True)
            self.logger().network(
                "Unexpected error cancelling orders.",
                exc_info=True,
                app_warning_msg="Failed to cancel order on Probit.com. Check API key and network connection."
            )

        failed_cancellations = [CancellationResult(oid, False) for oid in order_id_set]
        return successful_cancellations + failed_cancellations

    def tick(self, timestamp: float):
        """
        Is called automatically by the clock for each clock's tick (1 second by default).
        It checks if status polling task is due for execution.
        """
        now = time.time()
        poll_interval = (self.SHORT_POLL_INTERVAL
                         if now - self._user_stream_tracker.last_recv_time > 60.0
                         else self.LONG_POLL_INTERVAL)
        last_tick = self._last_timestamp / poll_interval
        current_tick = timestamp / poll_interval

        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp
        self._current_timestamp = timestamp

    async def test_ticker(self):
        ''' Temporary function for connector testing '''
        while True:
            self.tick(time.time())
            await asyncio.sleep(2)

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = s_decimal_NaN) -> TradeFee:
        """
        To get trading fee, this function is simplified by using fee override configuration. Most parameters to this
        function are ignore except order_type. Use OrderType.LIMIT_MAKER to specify you want trading fee for
        maker order.
        """
        is_maker = order_type is OrderType.LIMIT_MAKER
        return TradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _user_stream_event_listener(self):
        """
        Listens to message in _user_stream_tracker.user_stream queue.
        """
        # Probit exchange does not have websocket API, just pass.
        pass
