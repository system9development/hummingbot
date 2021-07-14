import asyncio
import logging
from hummingbot.connector.exchange.probit.probit_api_order_book_data_source import ProbitAPIOrderBookDataSource


# NOTE: This def is just here for debugging
async def main():

    logging.basicConfig(filename="test.log", filemode="w+", level=logging.DEBUG)

    logger = logging.getLogger()

    # Example of unauthenticated req (only channel not needing auth is "marketdata")
    # cli_sock = ProbitWebsocket()
    # await cli_sock.connect()
    # await cli_sock.request(type_sub_or_unsub = "subscribe", channel = "marketdata", params = {"interval": 500})

    # Example of authenticated req
    # NOTE: We must pass a ProbitAuth object into the WS constructor for any authenticated req's

    # cli_sock = ProbitWebsocket(auth = ProbitAuth())
    # print("Websocket created")
    # await cli_sock.connect()
    # print("Websocket connected")
    # await cli_sock.request(type_sub_or_unsub = "subscribe", channel = "trade_history")

    # async for msg in cli_sock._messages():
    #     print(msg)

    # Testing probit_order_book_tracker with _probit_websocket and probit_api_order_book_data_source
    test_data_source = ProbitAPIOrderBookDataSource(trading_pairs = ["ETH-BTC", "LTC-BTC"])
    test_queue = asyncio.Queue()
    event_loop = asyncio.get_event_loop()

    logger.debug(f"Results of get_last_traded_prices are: {await test_data_source.get_last_traded_prices(test_data_source._trading_pairs)}")

    logger.debug(f"Results of get_new_order_book are: {(await test_data_source.get_new_order_book(test_data_source._trading_pairs[0])).snapshot}")

    logger.debug(f"Results of get_snapshot are: {await test_data_source.get_snapshot(test_data_source._trading_pairs[0])}")

    # How to test out listen_for_orderbook_snapshots?
    logger.debug(f"Results of listen_for_orderbook_snapshots are: {await test_data_source.listen_for_order_book_snapshots(ev_loop = event_loop, output = test_queue)}")

    logger.debug(f"Results of listen_for_trades are: {await test_data_source.listen_for_trades()}")

    while True:
        print(item for item in test_queue)

asyncio.run(main())
