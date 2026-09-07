# Stocoo TickReplay Terminal

An institutional-grade web trading and charting workstation powered by **OpenAlgo Charts 2.0**, Canvas2D rendering, high-precision **400-row Fixed Range Volume Profile (FRVP)**, variable-speed tick replay simulation, and a live real-time market data WebSocket bridge.

---

## Highlights & Features

- **OpenAlgo Charts 2.0 Engine**:
  - Ultra-fast 60 FPS Canvas2D candlestick chart with sub-millisecond panning and zooming.
  - Multi-timeframe dynamic aggregation (1s, 5s, 10s, 30s, 1m, 3m, 5m, 15m, 30m, 1h, 4h, 1d).
  - Pre-aggregated 1-minute bar caching for instantaneous multi-timeframe switching in <15ms.
- **Institutional Drawing Toolkit (51 Tools)**:
  - Trendlines, Rays, Parallel Channels, Pitchforks, Fibonacci Retracements, Gann Boxes, Anchored Text, Brush, and Risk/Reward position calculators.
  - Weak-magnet vertex snapping for precise wick and body alignment.
  - Full localStorage persistence across trading sessions.
- **400-Row Fixed Range Volume Profile (FRVP)**:
  - High-density price row resolution with dynamic tick size normalization.
  - CME Group 70% Value Area greedy expansion (VAH/VAL) and Point of Control (POC).
  - Auction Market Theory (AMT) weighting (72% body / 28% wicks).
  - Floating Quick-HUD dock with interactive profile width pills (20% to 100%) and alignment toggle.
- **Dual Mode: Tick Replay & Live Streaming**:
  - **Historical Tick Replay**: Variable speed tick playback (1x to 1000x), step-by-step forward/backward navigation, and order execution simulator.
  - **Live Real-Time Streaming**: Native WebSocket endpoint (/ws/live) streaming live cryptocurrency trades and candle updates.
- **Pure Web Architecture**:
  - Zero bloated desktop installers, zero native compilation required.
  - Lightweight single-binary or standard Python FastAPI backend.
  - PWA support with local Font Awesome 6 icons and offline manifest.

---

## Quick Start

### 1. Prerequisites
- Python 3.9+
- Modern Web Browser (Chrome, Edge, Firefox, Safari)

### 2. Installation
Clone the repository and install dependencies:
`ash
git clone https://github.com/chamith-Hemisara/stocoo-tickreplay.git
cd stocoo-tickreplay

pip install -r requirements.txt
`

### 3. Launch the Workstation
`ash
python server.py
`
*Or simply double-click un.bat on Windows.*

The server will automatically open **http://localhost:8765** in your default browser.

---

## Project Structure

`	ext
stocoo-tickreplay/
├── .gitignore              # Ignores large datasets and temporary caches
├── README.md               # Project overview and documentation
├── requirements.txt        # Minimal Python dependencies
├── run.bat                 # One-click Windows runner
├── server.py               # FastAPI backend (REST API + WebSocket)
├── index.html              # OpenAlgo Charts 2.0 Web Terminal UI
├── manifest.json           # PWA web manifest
├── sw.js                   # Service worker cache manager
├── icons/                  # PWA and browser application icons
├── vendor/                 # Local Font Awesome 6 icon assets
└── services/
    ├── data_storage.py     # SQLite bar aggregation and tick retrieval
    └── binance_service.py  # Binance Vision downloader & live WebSocket bridge
`

---

## API Reference

| Route | Method | Description |
| :--- | :--- | :--- |
| / | GET | Serves the interactive trading terminal UI |
| /api/symbols | GET | Lists all cached symbols and file sizes |
| /api/symbols/details | GET | Detailed metadata (date ranges, row counts, MB) |
| /api/bars?symbol=...&tf=1m | GET | Vector-resampled OHLCV candlestick data |
| /api/ticks?symbol=... | GET | High-frequency raw tick stream for replay |
| /api/symbols_catalog | GET | Browse downloadable instruments from Binance Vision |
| /api/download/start | POST | Asynchronous historical tick data downloader |
| /ws/live?symbol=BTCUSDT | WebSocket | Live candle & tick feed |

---

## Data Ingestion

To populate data for historical replay:
1. Open the terminal at http://localhost:8765.
2. Click **Download** in the top navigation bar.
3. Select an instrument (e.g., BTCUSDT), choose the date range, and click **Start Download**.
4. The backend downloads raw exchange ticks, builds pre-aggregated 1-minute index tables, and makes the symbol immediately replayable.

---

## License
Proprietary & Confidential. All rights reserved.
