SELECT

  time AS "time",
  close
FROM kline_trade_data_bin
WHERE
  symbol = 'ETHUSDT'
  and time BETWEEN '2022-08-17T07:19:30.397Z' AND '2022-09-16T07:19:30.397Z'
ORDER BY 1;