Set WshShell = CreateObject("WScript.Shell") 
WshShell.Run "cmd /c cd /d ""G:\SMA Pro"" && python  server.py >> ""G:\SMA Pro\sma.log"" 2>&1", 0, False 
