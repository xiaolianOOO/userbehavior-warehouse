@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ========================================
echo   UserBehavior Data Warehouse Dashboard
echo ========================================
echo.

if not exist ".venv\Scripts\python.exe" (
    echo [ERROR] .venv not found.
    echo Setup:
    echo     python -m venv .venv
    echo     .venv\Scripts\python.exe -m pip install -r requirements.txt
    echo.
    pause
    exit /b 1
)

echo Starting Streamlit on http://localhost:8501
echo Press Ctrl+C in this window to stop.
echo.

.venv\Scripts\python.exe -m streamlit run scripts\13_app_dashboard.py

echo.
echo Server stopped.
pause