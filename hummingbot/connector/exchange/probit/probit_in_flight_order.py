from decimal import Decimal
from typing import (
    Any,
    Dict,
    Optional,
)
import asyncio
from hummingbot.core.event.events import (
    OrderType,
    TradeType
)
from hummingbot.connector.in_flight_order_base import InFlightOrderBase


class ProbitInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: Optional[str],
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 initial_state: str = "open"):
        super().__init__(
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            initial_state,
        )
        self.trade_id_set = set()
        self.cancelled_event = asyncio.Event()

    @property
    def is_done(self) -> bool:
        return self.last_state in {"filled", "cancelled"}

    @property
    def is_failure(self) -> bool:
        return self.last_state in {"rejected"}

    @property
    def is_cancelled(self) -> bool:
        return self.last_state in {"cancelled"}

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> InFlightOrderBase:
        """
        :param data: json data from API
        :return: formatted InFlightOrder
        """
        retval = ProbitInFlightOrder(
            data["client_order_id"],
            data["exchange_order_id"],
            data["trading_pair"],
            getattr(OrderType, data["order_type"]),
            getattr(TradeType, data["trade_type"]),
            Decimal(data["price"]),
            Decimal(data["amount"]),
            data["last_state"]
        )
        retval.executed_amount_base = Decimal(data["executed_amount_base"])
        retval.executed_amount_quote = Decimal(data["executed_amount_quote"])
        retval.fee_asset = data["fee_asset"]
        retval.fee_paid = Decimal(data["fee_paid"])
        retval.last_state = data["last_state"]
        return retval

    def update_with_trade_update(self, trade_update: Dict[str, Any]) -> bool:
        """
        Updates the in flight order with trade update (from /myTrades end point)
        return: True if the order gets updated otherwise False
        """

        trade_id = trade_update["id"]
        # trade_update["orderId"] is type int
        if str(trade_update["order_id"]) != self.exchange_order_id or trade_id in self.trade_id_set:
            # trade already recorded
            return False
        self.trade_id_set.add(trade_id)

        # Do not update executed amounts in filled orders
        if self.last_state != 'filled':
            self.executed_amount_base += Decimal(str(trade_update["quantity"]))
            self.executed_amount_quote += (Decimal(str(trade_update["price"])) *
                                           Decimal(str(trade_update["quantity"])))
        # Update fee info
        self.fee_asset = trade_update['fee_currency_id']
        self.fee_paid += Decimal(trade_update["fee_amount"])

        return True

    def get_exchange_order_id(self):
        return self.exchange_order_id

    def get_client_order_id(self):
        return self.client_order_id
