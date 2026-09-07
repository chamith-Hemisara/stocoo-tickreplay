import os
import sys
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict, Tuple
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent

def find_db_path() -> Path:
    candidates = [
        BASE_DIR / "data" / "tick_database.sqlite",
        BASE_DIR / "output" / "tick_database.sqlite",
        BASE_DIR / "tick_database.sqlite",
        Path("F:/Algo Project/Ninja Trader Data Downloader/output/tick_database.sqlite"),
        Path.cwd() / "output" / "tick_database.sqlite",
        Path.cwd() / "tick_database.sqlite",
    ]
    for c in candidates:
        if c.exists():
            return c.resolve()
    return (BASE_DIR / "data" / "tick_database.sqlite").resolve()

_base_1m_cache: Dict[str, pd.DataFrame] = {}
_bars_cache: Dict[Tuple[str, int], List[dict]] = {}

def get_db_connection() -> sqlite3.Connection:
    db_path = find_db_path()
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found at {db_path}")
    return sqlite3.connect(db_path)

def list_symbols() -> List[dict]:
    db_path = find_db_path()
    if not db_path.exists():
        return []

    symbols = []
    con = sqlite3.connect(db_path)
    try:
        cursor = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        for row in cursor.fetchall():
            table_name = row[0]
            if table_name.endswith("_Last") or table_name.endswith("_Bid"):
                sym = table_name.rsplit("_", 1)[0]
                count = con.execute(f'SELECT count(*) FROM "{table_name}"').fetchone()[0]
                size_mb = (count * 24) / (1024 * 1024)
                symbols.append({
                    "name": sym,
                    "filename": table_name,
                    "size_mb": round(size_mb, 2),
                })
    finally:
        con.close()
    return sorted(symbols, key=lambda x: x["name"])

def get_symbol_details() -> dict:
    db_path = find_db_path()
    if not db_path.exists():
        return {"symbols": [], "db_path": str(db_path), "total_size_mb": 0}

    total_size_mb = round(os.path.getsize(db_path) / (1024 * 1024), 2) if os.path.exists(db_path) else 0
    symbols = []
    con = sqlite3.connect(db_path)
    try:
        cursor = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cursor.fetchall()]
        sym_set = set()
        for t in tables:
            if t.endswith("_Last") or t.endswith("_Bid"):
                sym_set.add(t.rsplit("_", 1)[0])

        for sym in sorted(sym_set):
            table_name = f"{sym}_Last" if f"{sym}_Last" in tables else f"{sym}_Bid"
            tick_count = con.execute(f'SELECT count(*) FROM "{table_name}"').fetchone()[0]

            bar_table = f"{sym}_1m"
            bars_count = 0
            if bar_table in tables:
                bars_count = con.execute(f'SELECT count(*) FROM "{bar_table}"').fetchone()[0]

            min_ts = max_ts = None
            if tick_count > 0:
                row_min = con.execute(f'SELECT min(ts), max(ts) FROM "{table_name}"').fetchone()
                if row_min and row_min[0]:
                    min_ts = datetime.fromtimestamp(row_min[0] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                    max_ts = datetime.fromtimestamp(row_min[1] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")

            size_mb = round((tick_count * 24) / (1024 * 1024), 2)
            symbols.append({
                "name": sym,
                "table": table_name,
                "tick_count": tick_count,
                "bars_1m": bars_count,
                "start_date": min_ts,
                "end_date": max_ts,
                "size_mb": size_mb,
            })
    finally:
        con.close()

    return {
        "symbols": symbols,
        "db_path": str(db_path),
        "total_size_mb": total_size_mb,
    }

def get_bars(symbol: str, interval_seconds: int, from_ts: Optional[int] = None, to_ts: Optional[int] = None, max_bars: Optional[int] = None) -> List[dict]:
    cache_key = (symbol, interval_seconds)
    bars = None

    if cache_key in _bars_cache:
        bars = _bars_cache[cache_key]
    elif interval_seconds >= 60 and symbol in _base_1m_cache:
        df_1m = _base_1m_cache[symbol]
        if interval_seconds == 60:
            bars = df_1m.to_dict(orient="records")
        else:
            df_resampled = df_1m.copy()
            df_resampled["t_group"] = (df_resampled["time"] // interval_seconds) * interval_seconds
            grouped = df_resampled.groupby("t_group", as_index=False).agg(
                open=("open", "first"),
                high=("high", "max"),
                low=("low", "min"),
                close=("close", "last"),
                volume=("volume", "sum")
            ).rename(columns={"t_group": "time"})
            bars = grouped.to_dict(orient="records")
        _bars_cache[cache_key] = bars
    else:
        db_path = find_db_path()
        if not db_path.exists():
            raise FileNotFoundError("Database not found")

        con = sqlite3.connect(db_path)
        try:
            bar_table = f"{symbol}_1m"
            has_bar_table = con.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{bar_table}'").fetchone()

            if interval_seconds >= 60 and has_bar_table:
                df_1m = pd.read_sql_query(f'SELECT time, open, high, low, close, volume FROM "{bar_table}" ORDER BY time ASC', con)
                _base_1m_cache[symbol] = df_1m
                _bars_cache[(symbol, 60)] = df_1m.to_dict(orient="records")

                if interval_seconds == 60:
                    bars = _bars_cache[(symbol, 60)]
                else:
                    df_resampled = df_1m.copy()
                    df_resampled["t_group"] = (df_resampled["time"] // interval_seconds) * interval_seconds
                    grouped = df_resampled.groupby("t_group", as_index=False).agg(
                        open=("open", "first"),
                        high=("high", "max"),
                        low=("low", "min"),
                        close=("close", "last"),
                        volume=("volume", "sum")
                    ).rename(columns={"t_group": "time"})
                    bars = grouped.to_dict(orient="records")
                    _bars_cache[cache_key] = bars
            else:
                table_name = f"{symbol}_Last"
                if not con.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'").fetchone():
                    table_name = f"{symbol}_Bid"

                df = pd.read_sql_query(f'SELECT ts, price, volume FROM "{table_name}" ORDER BY ts ASC', con)
                df["time"] = (df["ts"] // (interval_seconds * 1000)) * interval_seconds
                grouped = df.groupby("time", as_index=False).agg(
                    open=("price", "first"),
                    high=("price", "max"),
                    low=("price", "min"),
                    close=("price", "last"),
                    volume=("volume", "sum")
                )
                bars = grouped.to_dict(orient="records")
                _bars_cache[cache_key] = bars
        finally:
            con.close()

    if not bars:
        return []

    if from_ts is not None or to_ts is not None:
        first_time = bars[0]["time"]
        last_time = bars[-1]["time"]
        f_ts = from_ts if from_ts is not None else first_time
        t_ts = to_ts if to_ts is not None else last_time

        if f_ts <= last_time and t_ts >= first_time:
            filtered = [b for b in bars if (from_ts is None or b["time"] >= from_ts) and (to_ts is None or b["time"] <= to_ts)]
            if filtered:
                bars = filtered

    if max_bars and len(bars) > max_bars:
        step = max(1, len(bars) // max_bars)
        bars = bars[::step]

    return bars

def get_ticks(symbol: str, start: int = 0, limit: int = 100000) -> List[Tuple[int, float, float]]:
    db_path = find_db_path()
    con = sqlite3.connect(db_path)
    try:
        table_name = f"{symbol}_Last"
        if not con.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'").fetchone():
            table_name = f"{symbol}_Bid"
        cursor = con.execute(f'SELECT ts, price, volume FROM "{table_name}" ORDER BY ts ASC LIMIT {limit} OFFSET {start}')
        return [(int(row[0] / 1000), float(row[1]), float(row[2])) for row in cursor.fetchall()]
    finally:
        con.close()
