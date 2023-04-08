import os
import talib
import json
import numpy as np
import pandas as pd
from binance.client import Client
from binance.exceptions import BinanceAPIException
import config
from multiprocessing import Pool
+from threading import Thread
import websockets as websocket


# Initialize the Binance client
client = Client(config.API_KEY, config.API_SECRET)

# Trading parameters
timeframe = Client.KLINE_INTERVAL_1HOUR
start_time = '1 year ago UTC'
end_time = 'now UTC'

class BinanceAPIError(Exception):
    pass

def get_historical_klines(symbol, interval, start_time, end_time):
    try:
        klines = client.get_historical_klines(symbol, interval, start_time, end_time)
        ohlc = [(
            float(entry[1]), # open
            float(entry[2]), # high
            float(entry[3]), # low
            float(entry[4])  # close
        ) for entry in klines]
        return pd.DataFrame(ohlc, columns=['open', 'high', 'low', 'close'])
    except BinanceAPIException as e:
        raise BinanceAPIError(str(e))

def apply_technical_indicators(data):
    # RSI
    data['RSI'] = talib.RSI(data['close'], timeperiod=14)

    # MACD
    macd, signal, hist = talib.MACD(data['close'], fastperiod=12, slowperiod=26, signalperiod=9)
    data['macd'] = macd
    data['macdsignal'] = signal
    data['macdhist'] = hist

    # Stochastics
    slowk, slowd = talib.STOCH(data['high'], data['low'], data['close'], fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
    data['slowk'] = slowk
    data['slowd'] = slowd

    # Bollinger Bands
    data['upper_band'], data['middle_band'], data['lower_band'] = talib.BBANDS(data['close'], timeperiod=20)

    # Parabolic SAR
    data['parabolic_sar'] = talib.SAR(data['high'], data['low'], acceleration=0.02, maximum=0.2)

    # Average True Range
    data['ATR'] = talib.ATR(data['high'], data['low'], data['close'], timeperiod=14)

    return data

def generate_signals(data):
    buy_signals = []
    sell_signals = []

    position = False

    for i in range(len(data)):
        # Check for buy signal
        if (data['RSI'][i] < 30) and (data['macd'][i] > data['macdsignal'][i]) and (data['slowk'][i] < 20) and (data['close'][i] < data['lower_band'][i]) and not position:
            buy_signals.append((data.index[i], data['close'][i]))
            position = True
        # Check for sell signal
        elif (data['RSI'][i] > 70) and (data['macd'][i] < data['macdsignal'][i]) and (data['slowk'][i] > 80) and (
                data['close'][i] > data['upper_band'][i]) and position:
            sell_signals.append((data.index[i], data['close'][i]))
            position = False
    return buy_signals, sell_signals

def calculate_fib_levels(data):
    max_price = data['high'].max()
    min_price = data['low'].min()

    diff = max_price - min_price

    # Fibonacci levels
    levels = [0.236, 0.382, 0.5, 0.618, 0.786]
    fib_levels = {level: min_price + diff * level for level in levels}

    return fib_levels

def get_binance_futures_symbols():
    exchange_info = client.get_exchange_info()
    symbols = [symbol_data['symbol'] for symbol_data in exchange_info['symbols'] if symbol_data['quoteAsset'] == 'USDT' and symbol_data['symbol'][-4:] == 'USDT']
    return symbols

def process_coin(coin):
    try:
        print(f"Processing {coin}")
        data = get_historical_klines(coin, timeframe, start_time, end_time)
        if data is None:
            return None

        data = apply_technical_indicators(data)
        buy_signals, sell_signals = generate_signals(data)
        fib_levels = calculate_fib_levels(data)

        if len(buy_signals) > 0 and len(sell_signals) > 0:
            return f"Coin: {coin}, Buy Signals: {len(buy_signals)}, Sell Signals: {len(sell_signals)}, Fib Levels: {fib_levels}"
        else:
            return None
    except Exception as e:
        print(f"Error processing {coin}: {e}")
        return None

def on_open(ws):
    print("WebSocket opened")

def on_message(ws, message):
    json_message = json.loads(message)
    kline = json_message['k']
    symbol = json_message['s']
    close = kline['c']
    print(f"Received update for {symbol}: {close}")

def on_close(ws):
    print(f"WebSocket closed")


def main():
    futures_coins = get_binance_futures_symbols()

    with Pool() as pool:
        results = pool.map(process_coin, futures_coins)

    for result in results:
        if result is not None:
            print(result)

    # Initialize WebSocket for real-time updates
    socket = "wss://stream.binance.com:9443/stream?streams="

    symbols_to_monitor = [result.split(" ")[-1] for result in results if result is not None]
    for symbol in symbols_to_monitor:
        socket += f"{symbol.lower()}@kline_{timeframe}/"

    ws = websocket.WebSocketApp(socket, on_open=on_open, on_message=on_message, on_close=on_close, header=None)
    thread = Thread(target=ws.run_forever)
    thread.start()

if __name__ == "__main__":
    main()
