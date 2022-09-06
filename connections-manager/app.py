from binance.websocket.spot.websocket_client import SpotWebsocketClient as WebsocketClient
from binance.spot import Spot as Client
from datetime import datetime


base_urls = [   'https://api1.binance.com'
                'https://api2.binance.com',
                'https://api3.binance.com',]


def message_handler(message_full):
    message = message_full['data']
    if 'e' in message and message['e'] == 'error':
        print('X_X_X_X__X_X__X_X__X_X_X_X__X_X__X_X_X')
        print(message)
        print('X_X_X_X__X_X__X_X__X_X_X_X__X_X__X_X_X')
    else:
        if 'k' in message and message['k']['x']:
            print(message)
   
ws_client = WebsocketClient()
ws_client.start()


# Combine selected streams
ws_client.instant_subscribe(
    stream=['bnbusdt@kline_1m','btcusdt@kline_1m', 'ethusdt@kline_1m'],
    callback=message_handler,
)

# time.sleep(5)
# ws_client.stop()