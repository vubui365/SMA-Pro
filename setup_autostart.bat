@echo off
chcp 65001 >nul 2>&1
cd /d "%~dp0"
echo ============================================================
echo   SMA Pro - Cai dat tu dong khoi dong cung Windows
echo   (Can chay voi quyen Administrator)
echo ============================================================
echo.

REM Kiem tra quyen Admin
net session >nul 2>&1
if errorlevel 1 (
    echo LOI: Can chay file nay voi quyen Administrator!
    echo Right-click file nay → "Run as administrator"
    pause
    exit /b 1
)

REM Thu muc hien tai (chua server_v9999.py)
set SMA_DIR=%~dp0
set SMA_DIR=%SMA_DIR:~0,-1%

REM Kiem tra file can thiet
if not exist "%SMA_DIR%\server.py" (
    echo LOI: Khong tim thay server.py trong %SMA_DIR%
    pause & exit /b 1
)
if not exist "%SMA_DIR%\index.html" (
    echo LOI: Khong tim thay index.html trong %SMA_DIR%
    pause & exit /b 1
)
if not exist "%SMA_DIR%\ngrok.exe" (
    echo CANH BAO: Khong tim thay ngrok.exe - se bo qua task Ngrok
    set SKIP_NGROK=1
) else (
    set SKIP_NGROK=0
)

REM Tim python
set PYTHON_CMD=
python3 --version >nul 2>&1
if not errorlevel 1 (set PYTHON_CMD=python3 & goto :found_py)
python --version >nul 2>&1
if not errorlevel 1 (set PYTHON_CMD=python & goto :found_py)
py --version >nul 2>&1
if not errorlevel 1 (set PYTHON_CMD=py & goto :found_py)
echo LOI: Khong tim thay Python!
pause & exit /b 1

:found_py
echo Tim thay Python: %PYTHON_CMD%
echo Thu muc SMA   : %SMA_DIR%
echo.

REM Tao VBScript de chay SMA server am thanh (khong hien CMD window)
set VBS_SERVER=%SMA_DIR%\sma_server_start.vbs
echo Set WshShell = CreateObject("WScript.Shell") > "%VBS_SERVER%"
echo WshShell.Run "cmd /c cd /d ""%SMA_DIR%"" && %PYTHON_CMD% server.py >> ""%SMA_DIR%\sma.log"" 2>&1", 0, False >> "%VBS_SERVER%"
echo     VBScript server tao: %VBS_SERVER%

REM Tao VBScript de chay Ngrok am thanh
set VBS_NGROK=%SMA_DIR%\sma_ngrok_start.vbs
echo Set WshShell = CreateObject("WScript.Shell") > "%VBS_NGROK%"
echo WshShell.Run "cmd /c cd /d ""%SMA_DIR%"" && ngrok http --domain=dermatophytic-arlie-nonorthodoxly.ngrok-free.dev http://127.0.0.1:1349 >> ""%SMA_DIR%\ngrok.log"" 2>&1", 0, False >> "%VBS_NGROK%"
echo     VBScript ngrok  tao: %VBS_NGROK%
echo.

REM Tao Scheduled Task cho Server
echo [1/3] Tao task tu dong chay SMA Pro Server...
schtasks /delete /tn "SMA_Server" /f >nul 2>&1
schtasks /create /tn "SMA_Server" /tr "wscript.exe \"%VBS_SERVER%\"" /sc onlogon /delay 0000:30 /rl highest /f >nul 2>&1
if errorlevel 1 (echo     THAT BAI!) else (echo     Thanh cong!)

REM Tao Scheduled Task cho Ngrok
echo [2/3] Tao task tu dong chay Ngrok Tunnel...
if "%SKIP_NGROK%"=="1" (
    echo     Bo qua - khong co ngrok.exe
) else (
    schtasks /delete /tn "SMA_Ngrok" /f >nul 2>&1
    schtasks /create /tn "SMA_Ngrok" /tr "wscript.exe \"%VBS_NGROK%\"" /sc onlogon /delay 0001:00 /rl highest /f >nul 2>&1
    if errorlevel 1 (echo     THAT BAI!) else (echo     Thanh cong!)
)

REM Mo firewall port 1349
echo [3/3] Them Windows Firewall rule port 1349...
netsh advfirewall firewall delete rule name="SMA Pro" >nul 2>&1
netsh advfirewall firewall add rule name="SMA Pro" dir=in action=allow protocol=TCP localport=1349 >nul 2>&1
if errorlevel 1 (echo     THAT BAI!) else (echo     Thanh cong!)

echo.
echo ============================================================
echo   HOAN TAT CAI DAT TU DONG KHOI DONG!
echo.
echo   SMA Pro se tu dong chay khi dang nhap Windows:
echo   - 30 giay sau dang nhap : Server khoi dong
echo   - 60 giay sau dang nhap : Ngrok tunnel khoi dong
echo.
echo   Dia chi truy cap:
echo   Local  : http://localhost:1349
echo   Mobile : https://dermatophytic-arlie-nonorthodoxly.ngrok-free.app
echo.
echo   File log:
echo   Server : %SMA_DIR%\sma.log
echo   Ngrok  : %SMA_DIR%\ngrok.log
echo.
echo   De go cai dat: chay remove_autostart.bat
echo   De bat tat task: Task Scheduler → SMA_Server / Ngrok
echo ============================================================
pause