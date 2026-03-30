@echo off
chcp 65001 >nul 2>&1
cd /d "%~dp0"
echo ============================================================
echo   SMA Pro - News + Events + Financials
echo   19 Indicators + Patterns + Mobile Web
echo   Port 1349
echo ============================================================
echo.
set PYTHON_CMD=
python3 --version >nul 2>&1
if not errorlevel 1 (set PYTHON_CMD=python3 & goto :found)
python --version >nul 2>&1
if not errorlevel 1 (set PYTHON_CMD=python & goto :found)
py --version >nul 2>&1
if not errorlevel 1 (set PYTHON_CMD=py & goto :found)
echo LOI: Khong tim thay Python!
pause & exit /b 1
:found
if not exist server.py (echo LOI: Khong tim thay server.py & pause & exit /b 1)
echo Khoi dong SMA Pro...
echo Web: http://localhost:1349
echo Ctrl+C de dung
echo.
%PYTHON_CMD% server.py
pause
