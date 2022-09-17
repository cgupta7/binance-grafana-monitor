import os
import zipfile
import pandas as pd
from helper import pandas_to_postgres
import datetime
import numpy as np
from parameters import postgres_config
from concurrent.futures import ThreadPoolExecutor
import sys


columns = [ "Open time","Open","High",
            "Low","Close","Volume",
            "Close time","Quote asset volume",
            "Number of trades","Taker buy base asset volume",
            "Taker buy quote asset volume","Ignore"]

final_columns = ['time','symbol','open','high','low','close','quantity','trades']

zip_loc = []

for root,sub_dirs,files in os.walk('./'):
    for file in files:
        if file.endswith('.zip'):
            # print(root,file)
            zip_loc.append(root+'/'+file)

# file = zip_loc[0]

start_year = 2022
start_month = 8

print(len(zip_loc))

def file_open_ulpoad(file):
    
    with zipfile.ZipFile(file, 'r') as archive:
        for file_ in archive.namelist():
            try:
                with archive.open(file_) as myfile:
                    if file_.endswith('csv'):
                        df = pd.read_csv(myfile,names=columns, header=None)
                        # csv = myfile.read()
                        if start_year:
                            if int(file_.split('-')[2])<start_year:
                                continue
                        if start_month:
                            if int(file_.split('-')[3].split('.')[0])<start_month:
                                continue
                        
                        print(file_)
                        symbol = file_.split('-')[0]
                        df['symbol'] = symbol

                        fin_df = df[['Close time', 'symbol', 'Open', 'High', 'Low', 'Close', 'Volume',  'Number of trades']].copy() # type:ignore
                        fin_df.columns = final_columns  # type:ignore
                        fin_df.dropna(axis=0,inplace=True)
                        fin_df['time'] = [datetime.datetime.fromtimestamp(int(time_val)/1000) for time_val in fin_df['time'] ]

                        try:
                            pandas_to_postgres(fin_df,postgres_config,'kline_trade_data_bin',['time','symbol'],verbose=1) #type:ignore

                        except:
                            _err = f"Issue while uploading data-{file_}: {e}" #type:ignore
                            # logger.error(_err)
                            print(_err)
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1] #type:ignore
                            print(exc_type, fname, exc_tb.tb_lineno) #type:ignore
                        return "read_success"
        
            except Exception as e:
                _err = f"Issue while data read-convert-{file_}: {e}"
                # logger.error(_err)
                print(_err)
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1] #type:ignore
                print(exc_type, fname, exc_tb.tb_lineno) #type:ignore
                return file_

with ThreadPoolExecutor(max_workers=50) as executor:
    for upload in executor.map(file_open_ulpoad,zip_loc):
        if upload:
            print(upload)


for upload in map(file_open_ulpoad,zip_loc):
    # print("in file --- ",zip_loc)
    if upload:
        print(upload)