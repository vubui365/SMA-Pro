@echo off
chcp 65001 >nul 2>&1
cd /d "%~dp0"
echo ============================================================
echo   SMA Pro - Cloudflare Tunnel
echo   Tao duong ham de truy cap tu internet
echo ============================================================
echo.

REM Kiem tra cloudflared da cai chua
cloudflared --version >nul 2>&1
if not errorlevel 1 goto :run_tunnel

echo [!] Chua cai cloudflared. Dang tai ve...
echo.

REM Tai cloudflared cho Windows
powershell -Command "& {Invoke-WebRequest -Uri 'https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe' -OutFile 'cloudflared.exe'}"

if not exist cloudflared.exe (
    echo LOI: Tai cloudflared that bai.
    echo Vao https://github.com/cloudflare/cloudflared/releases
    echo Tai file cloudflared-windows-amd64.exe ve thu muc nay
    pause
    exit /b 1
)
echo Da tai cloudflared.exe thanh cong!
echo.

:run_tunnel
echo [INFO] Dang khoi dong Cloudflare Tunnel...
echo [INFO] Server SMA phai dang chay tren port 1349
echo.
echo ============================================================
echo  SAU KHI TUNNEL CHAY:
echo  - Link se hien ra ngay ben duoi (dang *.trycloudflare.com)
echo  - Copy link do dan vao Settings SMA (public_url)
echo  - Nhan link cho nguoi dung dung tren dien thoai
echo ============================================================
echo.

REM Chay tunnel
cloudflared tunnel --url http://localhost:1349

echo.
echo Tunnel da dung.
pause
