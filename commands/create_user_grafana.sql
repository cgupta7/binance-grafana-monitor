CREATE USER grafanareader2 WITH PASSWORD 'read#123';
GRANT USAGE ON SCHEMA public TO grafanareader2;
GRANT SELECT ON public.kline_trade_data_bin TO grafanareader2;
GRANT SELECT ON public.ohlc_data_minute TO grafanareader2;