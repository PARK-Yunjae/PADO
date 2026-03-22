@echo off
title PADO Daily

REM ============================================
REM PADO Daily Auto-Run
REM Task Scheduler: 08:20 every day
REM Trading day: briefing > midday > CB > scan
REM Weekend: news + global only
REM ============================================

cd /d C:\Coding\PADO
call .venv\Scripts\activate.bat

echo.
echo === PADO START %date% %time% ===
echo.

python main.py

echo.
echo === PADO DONE %time% ===
timeout /t 10
