@echo off
title Stocoo TickReplay Web Terminal
echo ========================================================
echo Starting Stocoo TickReplay Web Terminal
echo ========================================================
echo.

cd /d "%~dp0"

echo [1/2] Checking Python dependencies...
python -m pip install -q -r requirements.txt

echo.
echo [2/2] Launching Web Server on http://localhost:8765...
python server.py

pause
