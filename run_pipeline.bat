@echo off
echo ============================================================
echo   Institutional V10 Quantitative Expected Return Pipeline
echo   Bayesian Sizing | Microstructure | Alpha Lifecycle Mgmt
echo ============================================================
echo.

cd /d "C:\Users\ASPHAJET\Documents\New folder\algo-trading-automation"

:: Run pre-flight regression tests first
echo [1/2] Running pre-flight verification...
python scripts\test_alpha_portfolio.py
if %ERRORLEVEL% NEQ 0 (
    echo [ABORT] Alpha engine tests failed. Fix before trading.
    pause
    exit /b 1
)

echo.
echo [2/2] Starting V10 Pipeline Orchestrator...
python src\pipeline\orchestrator.py

pause
