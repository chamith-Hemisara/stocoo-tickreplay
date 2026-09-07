import io
import json
import time
import zipfile
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, AsyncGenerator
import requests
import pandas as pd
import asyncio
import websockets

BINANCE_EXCHANGE_INFO = {
    "spot": "https://api.binance.com/api/v3/exchangeInfo",
    "um": "https://fapi.binance.com/fapi/v1/exchangeInfo",
    "cm": "https://dapi.binance.com/dapi/v1/exchangeInfo",
}

BINANCE_MARKET_URLS = {
    "spot": "https://data.binance.vision/data/spot/daily/aggTrades",
    "um": "https://data.binance.vision/data/futures/um/daily/aggTrades",
    "cm": "https://data.binance.vision/data/futures/cm/daily/aggTrades",
}

def fetch_symbols(market: str = "spot") -> List[str]:
    market = market.lower()
    if market not in BINANCE_EXCHANGE_INFO:
        market = "spot"
    url = BINANCE_EXCHANGE_INFO[market]
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        symbols = []
        for s in data.get("symbols", []):
            st = s.get("status", "") or s.get("contractStatus", "")
            if st == "TRADING":
                symbols.append(s.get("symbol"))
        return sorted(symbols)
    except Exception as e:
        print(f"Error fetching symbols: {e}")
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"]

def export_sqlite(df: pd.DataFrame, db_path: Path, symbol: str, data_type: str = "Last"):
    if df.empty:
        return db_path
    
    db_path.parent.mkdir(parents=True, exist_ok=True)
    df = df.copy()
    df["ts"] = pd.to_numeric(df["ts"], errors="coerce")
    df = df.dropna(subset=["ts", "price"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(1)
    df = df.dropna(subset=["price"])
    df = df.drop_duplicates(subset=["ts", "price", "volume"])
    df = df.sort_values("ts", ascending=True).reset_index(drop=True)

    table_name = f"{symbol}_{data_type}"
    con = sqlite3.connect(db_path)
    try:
        df.to_sql(table_name + "_temp", con, if_exists="replace", index=False)
        con.execute(f'''
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                ts INTEGER,
                price REAL,
                volume REAL,
                PRIMARY KEY (ts, price, volume)
            )
        ''')
        con.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_ts" ON "{table_name}" (ts)')
        con.execute(f'''
            INSERT OR IGNORE INTO "{table_name}" (ts, price, volume)
            SELECT ts, price, volume FROM "{table_name}_temp"
        ''')
        con.execute(f'DROP TABLE "{table_name}_temp"')

        # Maintain 1m bars
        bar_table = f"{symbol}_1m"
        df_bar = df.copy()
        df_bar["time"] = (df_bar["ts"] // 60000) * 60
        grp = df_bar.groupby("time", as_index=False).agg(
            open=("price", "first"),
            high=("price", "max"),
            low=("price", "min"),
            close=("price", "last"),
            volume=("volume", "sum")
        )
        grp.to_sql(bar_table + "_temp", con, if_exists="replace", index=False)
        con.execute(f'''
            CREATE TABLE IF NOT EXISTS "{bar_table}" (
                time INTEGER PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL
            )
        ''')
        con.execute(f'''
            INSERT OR REPLACE INTO "{bar_table}" (time, open, high, low, close, volume)
            SELECT time, open, high, low, close, volume FROM "{bar_table}_temp"
        ''')
        con.execute(f'DROP TABLE "{bar_table}_temp"')
        con.commit()
    finally:
        con.close()

def download_binance_vision(symbol: str, market: str, start_date: str, end_date: str, db_path: Path, log_fn=print):
    market = market.lower()
    base_url = BINANCE_MARKET_URLS.get(market, BINANCE_MARKET_URLS["spot"])
    
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    curr = start_dt
    all_frames = []
    session = requests.Session()
    session.headers.update({"User-Agent": "StocooTickReplay/1.0"})

    while curr <= end_dt:
        d_str = curr.strftime("%Y-%m-%d")
        url = f"{base_url}/{symbol}/{symbol}-aggTrades-{d_str}.zip"
        log_fn(f"Downloading {symbol} for {d_str}...")
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 200 and len(r.content) > 100:
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    for name in z.namelist():
                        if name.endswith(".csv"):
                            with z.open(name) as f:
                                # aggTrades cols: [aggTradeId, price, qty, firstTradeId, lastTradeId, timestamp, isBuyerMaker, isBestMatch]
                                df_day = pd.read_csv(f, header=None, usecols=[1, 2, 5], names=["price", "volume", "ts"])
                                all_frames.append(df_day)
            else:
                log_fn(f"No daily data for {d_str} (status {r.status_code})")
        except Exception as exc:
            log_fn(f"Error fetching {d_str}: {exc}")
        curr += timedelta(days=1)

    if not all_frames:
        log_fn("No data downloaded.")
        return 0

    log_fn("Merging and indexing data...")
    merged = pd.concat(all_frames, ignore_index=True)
    export_sqlite(merged, db_path, symbol, "Last")
    log_fn(f"Successfully saved {len(merged)} ticks to database.")
    return len(merged)

async def stream_binance_live(symbol: str, interval: str = "1m") -> AsyncGenerator[dict, None]:
    """Streams live candlestick updates directly from Binance WebSocket."""
    sym = symbol.lower()
    url = f"wss://stream.binance.com:9443/ws/{sym}@kline_{interval}"
    async with websockets.connect(url) as ws:
        while True:
            try:
                msg = await ws.recv()
                data = json.loads(msg)
                k = data.get("k", {})
                yield {
                    "type": "candle",
                    "symbol": symbol,
                    "time": int(k.get("t", 0) // 1000),
                    "open": float(k.get("o", 0)),
                    "high": float(k.get("h", 0)),
                    "low": float(k.get("l", 0)),
                    "close": float(k.get("c", 0)),
                    "volume": float(k.get("v", 0)),
                    "is_closed": k.get("x", False),
                }
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"WS error: {e}")
                await asyncio.sleep(1)
