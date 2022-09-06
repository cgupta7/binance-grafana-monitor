from decouple import config
from binance import ThreadedWebsocketManager

api_key = config("API_KEY")
api_secret = config("SECRET_KEY")

def main():
    symbol = "ETHBTC"
    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)  # type: ignore
    twm.start()

    def handle_message(msg):
        print(msg)

    twm.start_trade_socket(callback=handle_message,symbol=symbol)
    twm.join()

if __name__ == "__main__":
    main()