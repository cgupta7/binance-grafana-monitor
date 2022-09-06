CREATE TABLE IF NOT EXISTS kline_trade_data_bin (
time TIMESTAMP WITH TIME ZONE NOT NULL,
symbol text NOT NULL,
open double PRECISION NOT NULL,
high double PRECISION NOT NULL,
low double PRECISION NOT NULL,
close double PRECISION NOT NULL
quantity double PRECISION NOT NULL,
trades integer NOT NULL);

SELECT create_hypertable('kline_trade_data_bin','time');

CREATE MATERIALIZED VIEW ohlc_data_minute
WITH (timescaledb.continuous) AS
SELECT  symbol,
        time_bucket(INTERVAL '1 minute', time) as date,
        FIRST(open,time) as open,
        MAX(high) as high,
        MIN(low) as low,
        LAST(close,time) as close,
        SUM(quantity) as quantity,
        SUM(trades) as trades
from kline_trade_data_bin
GROUP BY symbol,date;