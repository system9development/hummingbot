
ACCOUNT = {'makerCommission': 0, 'takerCommission': 0, 'buyerCommission': 0, 'sellerCommission': 0, 'updateTime': None,
           'balances': [{'asset': 'usdt', 'free': '32.45', 'locked': '0'},
                        {'asset': 'xrp', 'free': '69.4', 'locked': '10'}]}

EXCHANGE_INFO = {'timezone': 'CTT', 'serverTime': 1602242940092,
                 'symbols': [
                     {'symbol': 'XRPUSDT', 'status': 'TRADING', 'baseAsset': 'xrp', 'baseAssetPrecision': 1, 'quoteAsset': 'usdt', 'quotePrecision': 5,
                      'orderTypes': ['MARKET', 'LIMIT'], 'icebergAllowed': False,
                      'filters': [
                          {'filterType': 'PRICE_FILTER', 'minPrice': '0.025575000000000000000', 'maxPrice': '0.332475000000000000000', 'priceScale': 5},
                          {'filterType': 'LOT_SIZE', 'minQty': '0.1000000000000000', 'maxQty': '1410065408.0000000000000000', 'volumeScale': 1}]}]
                 }


CREATE_ORDER = {'symbol': 'XRPUSDT', 'orderId': 1, 'clientOrderId': '', 'transactTime': 1602243590965}

NEW_ORDER = {'symbol': 'XRPUSDT', 'orderId': '1', 'clientOrderId': '', 'price': '0.2538700000000000', 'origQty': '10.0000000000000000',
             'executedQty': '0.0000000000000000', 'cummulativeQuoteQty': '0.0000000000000000', 'status': 'NEW',
             'timeInForce': '', 'type': 'LIMIT', 'side': 'SELL', 'stopPrice': '', 'icebergQty': '',
             'time': 1602243590000, 'updateTime': 1602243591000, 'isWorking': False}

FILLED_ORDER = {'symbol': 'XRPUSDT', 'orderId': '1', 'clientOrderId': '', 'price': '0.2538700000000000', 'origQty': '10.0000000000000000',
                'executedQty': '10.0000000000000000', 'cummulativeQuoteQty': '2.5387000000000000', 'status': 'FILLED',
                'timeInForce': '', 'type': 'LIMIT', 'side': 'SELL', 'stopPrice': '', 'icebergQty': '',
                'time': 1602450532000, 'updateTime': 1602450532000, 'isWorking': False}

CANCELED_ORDER = {'symbol': 'XRPUSDT', 'orderId': '1', 'clientOrderId': '', 'price': '0.2538700000000000', 'origQty': '10.0000000000000000',
                  'executedQty': '0.0000000000000000', 'cummulativeQuoteQty': '0.0000000000000000', 'status': 'CANCELED',
                  'timeInForce': '', 'type': 'LIMIT', 'side': 'SELL', 'stopPrice': '', 'icebergQty': '',
                  'time': 1602243590000, 'updateTime': 1602243591000, 'isWorking': False}

CANCEL_ORDER_RESPONSE = {'symbol': 'XRPUSDT', 'origClientOrderId': '', 'orderId': 1, 'clientOrderId': ''}

ORDER_BOOK = {'lastUpdateId': 1602244289424,
              'bids': [['0.25533', '1850.5', []], ['0.25532', '12737.8', []], ['0.25531', '527.3', []]],
              'asks': [['0.25534', '1415.6', []], ['0.25535', '12207.0', []], ['0.25536', '7485.8', []]]}

TRADES = [{'id': 31468986, 'price': '0.2552500000000000', 'qty': '74.4000000000000000', 'time': 1602448569000, 'isBuyerMaker': True, 'isBestMatch': True},
          {'id': 31468985, 'price': '0.2552600000000000', 'qty': '73.3000000000000000', 'time': 1602448567000, 'isBuyerMaker': False, 'isBestMatch': True}]

MY_TRADES = [
    {'symbol': 'XRPUSDT', 'id': 31369600, 'orderId': 305256785, 'origClientOrderId': '', 'price': '0.2539900000000000', 'qty': '10.0000000000000000',
     'commission': None, 'commissionAssert': None,
     'time': 1602241041000, 'isBuyer': True, 'isMaker': False, 'isBestMatch': True},
    {'symbol': 'XRPUSDT', 'id': 31344714, 'orderId': 304873911, 'origClientOrderId': '', 'price': '0.2519500000000000', 'qty': '10.0000000000000000',
     'commission': None, 'commissionAssert': None,
     'time': 1602188533000, 'isBuyer': False, 'isMaker': False, 'isBestMatch': True}]

NEW_TRADE = {'symbol': 'XRPUSDT', 'id': 31470342, 'orderId': 1, 'origClientOrderId': '', 'price': '0.2538700000000000', 'qty': '10.0000000000000000',
             'commission': None, 'commissionAssert': None, 'time': 1602450532000, 'isBuyer': False, 'isMaker': False, 'isBestMatch': True}

TICKER = {'symbol': 'XRPUSDT', 'price': '0.25535'}

PING = {}
