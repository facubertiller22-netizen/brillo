@echo off
echo.
echo  Cerrando servidor Brillo...
taskkill /F /IM python.exe >nul 2>&1
taskkill /F /IM uvicorn.exe >nul 2>&1

echo  Liberando puerto 8000...
for /f "tokens=5" %%a in ('netstat -aon ^| find ":8000" ^| find "LISTENING"') do (
    taskkill /F /PID %%a >nul 2>&1
)

echo  Limpiando archivos temporales de Python...
if exist __pycache__ rmdir /s /q __pycache__

echo.
echo  Reiniciando servidor Brillo...
start "" python start.py

echo.
echo  Listo. Brillo corriendo en http://localhost:8000
echo.
