# Binance Tick Data


## Usage

    from binance_tick import load_data, Kind
    df = load_data(kind=Kind.SPOT, symbol='ETHUSDT', start='2021-6-1', end='2021-9-1')

or

    df = load_data('ETHUSDT')

 You will get a pandas DataFrame with timestamps and lowest prices per second:

                        price
    datetime                    
    2021-02-28 23:59:25  1418.06
    2021-02-28 23:59:26  1418.01
    2021-02-28 23:59:27  1417.49
    2021-02-28 23:59:28  1417.20
    2021-02-28 23:59:29  1417.30
    ...                      ...
    2021-11-27 23:59:55  4098.46
    2021-11-27 23:59:56  4098.45
    2021-11-27 23:59:57  4097.32
    2021-11-27 23:59:58  4095.97
    2021-11-27 23:59:59  4095.13