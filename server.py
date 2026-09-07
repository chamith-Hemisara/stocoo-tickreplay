import os
import sys
import json
import asyncio
import webbrowser
from pathlib import Path
from typing import Optional, List
from datetime import datetime

from fastapi import FastAPI, Query, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uvicorn

# Project root
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from services.data_storage import (
    find_db_path,
    list_symbols,
    get_symbol_details,
    get_bars,
    get_ticks
)
from services.binance_service import (
    fetch_symbols as binance_fetch_symbols,
    download_binance_vision,
    stream_binance_live
)

app = FastAPI(
    title="Stocoo TickReplay Terminal",
    description="Professional OpenAlgo Charts 2.0 Terminal with Replay and Live Real-Time Data",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static mounts
vendor_dir = BASE_DIR / "vendor"
if vendor_dir.exists():
    app.mount("/vendor", StaticFiles(directory=str(vendor_dir)), name="vendor")

icons_dir = BASE_DIR / "icons"
if icons_dir.exists():
    app.mount("/icons", StaticFiles(directory=str(icons_dir)), name="icons")

TIMEFRAME_MAP = {
    "1s": 1,
    "5s": 5,
    "10s": 10,
    "30s": 30,
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}

# ---------------------------------------------------------------------------
# Frontend Delivery Routes
# ---------------------------------------------------------------------------

@app.get("/")
async def serve_index():
    index_file = BASE_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(404, "index.html not found")
    return FileResponse(index_file, media_type="text/html", headers={
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0"
    })

@app.get("/index.html")
async def serve_index_html():
    return await serve_index()

@app.get("/manifest.json")
async def serve_manifest():
    p = BASE_DIR / "manifest.json"
    if not p.exists():
        raise HTTPException(404, "manifest.json not found")
    return FileResponse(p, media_type="application/manifest+json")

@app.get("/sw.js")
async def serve_sw():
    p = BASE_DIR / "sw.js"
    if not p.exists():
        raise HTTPException(404, "sw.js not found")
    return FileResponse(p, media_type="application/javascript", headers={
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"
    })

# ---------------------------------------------------------------------------
# Chart Data Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/symbols")
async def api_symbols():
    return {"symbols": list_symbols()}

@app.get("/api/symbols/details")
async def api_symbols_details():
    return get_symbol_details()

@app.get("/api/bars")
async def api_bars(
    symbol: str = Query(..., description="Symbol name, e.g. BTCUSDT"),
    tf: str = Query("1m", description="Timeframe: 1s,5s,10s,30s,1m,3m,5m,15m,30m,1h,4h,1d"),
    from_ts: Optional[int] = Query(None, description="UTC seconds start (inclusive)"),
    to_ts: Optional[int] = Query(None, description="UTC seconds end (inclusive)"),
    max_bars: Optional[int] = Query(None, description="Max bars downsample"),
):
    if tf not in TIMEFRAME_MAP:
        raise HTTPException(400, f"Invalid timeframe: {tf}. Valid: {list(TIMEFRAME_MAP.keys())}")

    interval_sec = TIMEFRAME_MAP[tf]
    try:
        bars = get_bars(symbol, interval_sec, from_ts=from_ts, to_ts=to_ts, max_bars=max_bars)
        return {
            "symbol": symbol,
            "timeframe": tf,
            "interval_seconds": interval_sec,
            "count": len(bars),
            "bars": bars,
        }
    except FileNotFoundError:
        return {"symbol": symbol, "timeframe": tf, "count": 0, "bars": []}
    except Exception as exc:
        raise HTTPException(500, f"Failed to retrieve bars: {str(exc)}")

@app.get("/api/ticks")
async def api_ticks(
    symbol: str = Query(..., description="Symbol name"),
    start: Optional[int] = Query(0, description="Start index"),
    limit: Optional[int] = Query(10000, description="Max ticks to return"),
):
    try:
        ticks = get_ticks(symbol, start=start, limit=limit)
        return {
            "symbol": symbol,
            "total_returned": len(ticks),
            "start_index": start,
            "ticks": [{"time": t[0], "price": t[1], "volume": t[2]} for t in ticks]
        }
    except Exception as exc:
        raise HTTPException(500, f"Failed to retrieve ticks: {str(exc)}")

# ---------------------------------------------------------------------------
# Real-Time WebSocket Streaming
# ---------------------------------------------------------------------------

@app.websocket("/ws/live")
async def websocket_live_stream(websocket: WebSocket, symbol: str = "BTCUSDT", interval: str = "1m"):
    await websocket.accept()
    print(f"[WS] Client connected for {symbol} ({interval})")
    try:
        async for candle in stream_binance_live(symbol, interval):
            await websocket.send_text(json.dumps(candle))
    except WebSocketDisconnect:
        print(f"[WS] Client disconnected for {symbol}")
    except Exception as exc:
        print(f"[WS] Streaming error: {exc}")

# ---------------------------------------------------------------------------
# Downloader & Export
# ---------------------------------------------------------------------------

class DownloadRequest(BaseModel):
    provider: str = "binance"
    market: str = "spot"
    symbol: str
    start_date: str
    end_date: str

class DownloadManager:
    def __init__(self):
        self.status = "idle"
        self.logs: List[str] = []

    def log(self, msg: str):
        print(f"[DL] {msg}")
        self.logs.append(msg)
        if len(self.logs) > 100:
            self.logs = self.logs[-100:]

dl_mgr = DownloadManager()

def run_download_bg(req: DownloadRequest):
    try:
        dl_mgr.log(f"Starting {req.provider} download for {req.symbol} from {req.start_date} to {req.end_date}")
        db_path = find_db_path()
        download_binance_vision(
            symbol=req.symbol,
            market=req.market,
            start_date=req.start_date,
            end_date=req.end_date,
            db_path=db_path,
            log_fn=dl_mgr.log
        )
        dl_mgr.log("Download finished successfully.")
    except Exception as e:
        dl_mgr.log(f"Download error: {e}")
    finally:
        dl_mgr.status = "idle"

@app.get("/api/symbols_catalog")
async def api_symbols_catalog(provider: str = "binance", market: str = "spot"):
    syms = binance_fetch_symbols(market)
    return {"symbols": syms}

@app.post("/api/download/start")
async def api_download_start(req: DownloadRequest, bg: BackgroundTasks):
    if dl_mgr.status == "running":
        raise HTTPException(400, "Download already in progress")
    dl_mgr.status = "running"
    dl_mgr.logs = []
    bg.add_task(run_download_bg, req)
    return {"status": "started"}

@app.get("/api/download/status")
async def api_download_status():
    return {"status": dl_mgr.status, "logs": dl_mgr.logs}

@app.get("/api/all_symbols")
async def api_all_symbols(engine: str = "binance"):
    return await api_symbols_catalog(provider=engine)

@app.post("/api/download")
async def api_download_legacy(
    symbol: str = Query(...),
    start: str = Query(...),
    end: str = Query(...),
    market: str = Query("spot"),
    bg: BackgroundTasks = None
):
    req = DownloadRequest(provider="binance", market=market, symbol=symbol, start_date=start, end_date=end)
    return await api_download_start(req, bg)

# ---------------------------------------------------------------------------
# Server Startup
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = 8765
    print("=" * 65)
    print("  Stocoo TickReplay Terminal (Web Workstation)")
    print(f"  Database Location: {find_db_path()}")
    print(f"  Local URL:         http://localhost:{port}")
    print("=" * 65)

    if "--no-browser" not in sys.argv:
        try:
            webbrowser.open(f"http://localhost:{port}")
        except Exception:
            pass

    uvicorn.run(app, host="127.0.0.1", port=port, log_level="info")
