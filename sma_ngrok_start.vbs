Set WshShell = CreateObject("WScript.Shell") 
WshShell.Run "cmd /c cd /d ""G:\SMA Pro"" && ngrok http --domain=dermatophytic-arlie-nonorthodoxly.ngrok-free.dev http://127.0.0.1:1349 >> ""G:\SMA Pro\ngrok.log"" 2>&1", 0, False 
