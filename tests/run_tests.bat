@echo off
:: hpuft test runner for Windows
:: Activates the mamba/conda dev_env before running the Python test suite.
::
:: Usage:
::   run_tests.bat [extra args passed to test_hpuft.py]
::
:: Examples:
::   run_tests.bat
::   run_tests.bat --no-build
::   run_tests.bat --addr 192.168.50.9:9001 --recv-addr 192.168.50.9:9000
::   run_tests.bat --keep

setlocal

set SCRIPT_DIR=%~dp0

:: ── Activate mamba/conda dev_env ─────────────────────────────────────────────
call mamba activate dev_env 2>nul
if errorlevel 1 (
    call conda activate dev_env 2>nul
)

:: ── Run the test suite ────────────────────────────────────────────────────────
python "%SCRIPT_DIR%test_hpuft.py" %*
exit /b %errorlevel%
