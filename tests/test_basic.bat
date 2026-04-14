@echo off
setlocal

:: ── Configuration ────────────────────────────────────────────────────────────
set ADDR=192.168.50.9:9000
set EXE=hpuft.exe
set RECV_DIR=test_recv

:: Use a timestamp-based filename so every run is unique on the server.
for /f "tokens=1-6 delims=/:. " %%a in ("%date% %time%") do (
    set TS=%%a%%b%%c_%%d%%e%%f
)
set TEST_FILE=test_%TS%.txt
:: ─────────────────────────────────────────────────────────────────────────────

echo [test] Building...
go build -o %EXE% ./cmd/hpuft
if errorlevel 1 ( echo [test] BUILD FAILED & exit /b 1 )

:: Create test file
echo hello from hpuft test > %TEST_FILE%
echo [test] Created test file: %TEST_FILE%
type %TEST_FILE%
echo.

:: CertUtil checksum of the source file
echo [test] Source checksum:
certutil -hashfile %TEST_FILE% SHA256 | findstr /v "hash" | findstr /v "CertUtil"

:: Clean receive directory
if exist %RECV_DIR% rmdir /s /q %RECV_DIR%
mkdir %RECV_DIR%

echo.
echo ══════════════════════════════════════════════
echo  PUT
echo ══════════════════════════════════════════════
%EXE% put -addr %ADDR% -file %TEST_FILE%
if errorlevel 1 ( echo [test] PUT FAILED & goto :done )

echo.
echo ══════════════════════════════════════════════
echo  GET
echo ══════════════════════════════════════════════
%EXE% get -addr %ADDR% -file %TEST_FILE% -out %RECV_DIR%
if errorlevel 1 ( echo [test] GET FAILED & goto :done )

echo.
echo [test] Received file contents:
type %RECV_DIR%\%TEST_FILE%

echo [test] Received checksum:
certutil -hashfile %RECV_DIR%\%TEST_FILE% SHA256 | findstr /v "hash" | findstr /v "CertUtil"

echo.
echo [test] ALL TESTS PASSED

:done
:: Cleanup
if exist %TEST_FILE% del %TEST_FILE%
endlocal
