@echo off
chcp 65001 >nul 2>&1
cd /d "%~dp0"
echo ============================================================
echo   SMA Pro - Ultimate Analyzer (Port 1349)
echo   ML Ensemble + LSTM + Macro + Paper Trading + SSI RT
echo   Tunnel: Ngrok + Cloudflare (song song)
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
if not exist server.py  (echo LOI: Khong tim thay server.py  & pause & exit /b 1)
if not exist index.html (echo LOI: Khong tim thay index.html & pause & exit /b 1)

echo [1] Kiem tra / cai package can thiet...

%PYTHON_CMD% -c "import websocket" >nul 2>&1
if errorlevel 1 (
    echo     Cai websocket-client...
    %PYTHON_CMD% -m pip install websocket-client -q
    echo     OK
) else ( echo     websocket-client: da co. )

%PYTHON_CMD% -c "import sklearn" >nul 2>&1
if errorlevel 1 (
    echo     Cai scikit-learn...
    %PYTHON_CMD% -m pip install scikit-learn -q
    echo     OK
) else ( echo     scikit-learn: da co. )

%PYTHON_CMD% -c "import numpy" >nul 2>&1
if errorlevel 1 (
    echo     Cai numpy...
    %PYTHON_CMD% -m pip install numpy -q
    echo     OK
) else ( echo     numpy: da co. )

%PYTHON_CMD% -c "import scipy" >nul 2>&1
if errorlevel 1 (
    echo     Cai scipy...
    %PYTHON_CMD% -m pip install scipy -q
    echo     OK
) else ( echo     scipy: da co. )

%PYTHON_CMD% -c "import yfinance" >nul 2>&1
if errorlevel 1 (
    echo     Cai yfinance...
    %PYTHON_CMD% -m pip install yfinance -q
    echo     OK
) else ( echo     yfinance: da co. )

%PYTHON_CMD% -c "import tensorflow" >nul 2>&1
if errorlevel 1 (
    echo     [TUY CHON] TensorFlow chua co - LSTM dung numpy fallback.
    echo     De cai: pip install tensorflow
) else ( echo     TensorFlow: da co - LSTM day du. )

%PYTHON_CMD% -c "import redis" >nul 2>&1
if errorlevel 1 (
    echo     [TUY CHON] Redis chua co - cache se dung RAM.
    echo     De cai: pip install redis
) else ( echo     Redis: da co. )

echo.
echo [2] Them firewall rule port 1349...
netsh advfirewall firewall show rule name="SMA Pro" >nul 2>&1
if errorlevel 1 (
    netsh advfirewall firewall add rule name="SMA Pro" dir=in action=allow protocol=TCP localport=1349 >nul 2>&1
    echo     OK
) else ( echo     Da co san. )

echo [3] Khoi dong SMA Pro server...
start "SMA Pro Server" cmd /k "%PYTHON_CMD% server.py"
timeout /t 3 /nobreak >nul

echo [4] Khoi dong Ngrok (port 1349)...
if exist ngrok.exe (
    start "SMA Pro Ngrok" cmd /k "ngrok http --domain=dermatophytic-arlie-nonorthodoxly.ngrok-free.dev http://127.0.0.1:1349"
    echo     Ngrok: https://dermatophytic-arlie-nonorthodoxly.ngrok-free.dev
) else (
    echo     [BO QUA] Khong tim thay ngrok.exe
)

echo [5] Khoi dong Cloudflare Tunnel...
where cloudflared >nul 2>&1
if not errorlevel 1 (
    start "SMA Pro Cloudflare" cmd /k "cloudflared tunnel --url http://127.0.0.1:1349"
    echo     Cloudflare: xem cua so "SMA Pro Cloudflare" de lay URL
) else (
    echo     [BO QUA] Chua cai cloudflared
)

echo.
echo ============================================================
echo   DA KHOI DONG SMA Pro!
echo   Local    : http://localhost:1349
echo   Ngrok    : https://dermatophytic-arlie-nonorthodoxly.ngrok-free.dev
echo   CF Tunnel: xem cua so "SMA Pro Cloudflare" de lay URL
echo.
echo   Thu muc : G:\SMA Pro\
echo   Files can thiet (chi 4 file):
echo     server.py + index.html + sma.db + start_sma.bat
echo.
echo   --- API goc (v24) ---
echo   /api/v22/fundamental/VCB     du lieu co ban
echo   /api/v23/realtime/VCB        gia real-time SSI
echo   /api/v23/backtest_t2         backtest T+2
echo   /api/v23/lstm/train          train LSTM
echo   /api/v23/portfolio_backtest  backtest da ma
echo.
echo   --- API moi (v9999 tu v12) ---
echo   /api/macro                   USD/VND, P/E VNI, breadth
echo   /api/macro_context           macro suppress alert?
echo   /api/foreign_flow            dong tien nuoc ngoai RT
echo   /api/realtime_price/VCB      gia TCBS realtime
echo   /api/beta/VCB                Beta, Alpha vs VNINDEX
echo   /api/financial_summary/VCB   BCTC tom tat
echo   /api/paper_trades            paper trading summary
echo   /api/ml_train                train ML ensemble
echo   /api/ml_combined             ML + TA combined signal
echo   /api/backtest_regime         backtest theo market regime
echo   /api/portfolio_correlation   correlation matrix
echo   /api/portfolio_diversification  diem da dang hoa
echo ============================================================
pause
