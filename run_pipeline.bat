@echo off
echo ============================================================
echo   Institutional V15 Quantitative Expected Return Pipeline
echo   Bayesian Sizing ^| Microstructure ^| Alpha Lifecycle Mgmt
echo ============================================================
echo.

cd /d "%~dp0"

:: Run pre-flight regression tests first
echo [1/2] Running pre-flight verification...
.venv\Scripts\python -m unittest discover -s tests -p "test_*.py"
if %ERRORLEVEL% NEQ 0 (
    echo [ABORT] Expected return pipeline tests failed. Fix before trading.
    pause
    exit /b 1
)

echo.
echo [2/2] Starting V15 Pipeline Orchestrator...
.venv\Scripts\python src\pipeline\orchestrator.py

pause
