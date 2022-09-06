import datetime
from binance.websocket.spot.websocket_client import SpotWebsocketClient as WebsocketClient
from decouple import config
import psycopg2
import logging 

# logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
# logging.debug('This message should go to the log file')
# logging.info('So should this')
# logging.warning('And this, too')
# logging.error('And non-ASCII stuff, too, like Øresund and Malmö')

# logging.basicConfig(filename='app.log',filemode='w',format='%(asctime)s %(message)s', level=logging.DEBUG,datefmt='%d-%m-%Y %H:%M:%S %p')  
# logging.error('At this time we got an error') 
# logging.log(level = 0,msg = "At this time we got an error")

api_key = config('API_KEY')
api_secret = config('SECRET_KEY')
db_pass = config('DB_PASS')


base_urls = [   'https://api1.binance.com'
                'https://api2.binance.com',
                'https://api3.binance.com',]




def main():
    connection = psycopg2.connect(user="postgres",password=db_pass,host='localhost',port=5432,database="postgres")
    cursor = connection.cursor()

    
    
    ws_client = WebsocketClient()
    ws_client.start()
    sample_message = ""

    def message_handler(message_full:dict,cursor=cursor):
        message = message_full['data']
        if 'e' in message and message['e'] == 'error':
            print(message_full)
        else:
            if 'k' in message and message['k']['x']:
                
                query = "INSERT INTO kline_trade_data_bin (time,symbol,open,high,low,close,quantity,trades) " +\
                        "VALUES (%s, %s, %s, %s, %s ,%s ,%s, %s);"

                timestamp = datetime.datetime.fromtimestamp(message['k']['T']/1000)
                record = (  timestamp,message["s"],message["k"]["o"],message["k"]["h"],
                            message["k"]["l"],message["k"]["c"],message["k"]["v"],message["k"]["n"])

                cursor.execute(query,record)
                connection.commit()

                print(query,record)
                print('added')

                global sample_message
                sample_message = message

    # Combine selected streams
    ws_client.instant_subscribe(
        stream=['bnbusdt@kline_1s','btcusdt@kline_1s', 'ethusdt@kline_1s'],
        callback=message_handler,
    )

# time.sleep(5)
# ws_client.stop()

if __name__=='__main__':
    main()