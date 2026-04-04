@echo off
setlocal

:: ── Configuration ────────────────────────────────────────────────────────────
set ADDR=192.168.50.9:9000
set EXE=hpuft.exe
set RECV_DIR=test_recv

:: Use timestamp-based filenames so every run is unique on the server.
:: The C server rejects pushes if the filename already exists, so the plain
:: and encrypted pushes each get their own name.
for /f "tokens=1-6 delims=/:. " %%a in ("%date% %time%") do (
    set TS=%%a%%b%%c_%%d%%e%%f
)
set TEST_FILE=test_%TS%.txt
set TEST_FILE_ENC=test_%TS%_enc.txt
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
echo  PUSH (unencrypted)
echo ══════════════════════════════════════════════
%EXE% push -addr %ADDR% -file %TEST_FILE%
if errorlevel 1 ( echo [test] PUSH FAILED & goto :done )

echo.
echo ══════════════════════════════════════════════
echo  PULL (unencrypted)
echo ══════════════════════════════════════════════
%EXE% get -addr %ADDR% -file %TEST_FILE% -out %RECV_DIR%
if errorlevel 1 ( echo [test] PULL FAILED & goto :done )

echo.
echo [test] Received file contents:
type %RECV_DIR%\%TEST_FILE%

echo [test] Received checksum:
certutil -hashfile %RECV_DIR%\%TEST_FILE% SHA256 | findstr /v "hash" | findstr /v "CertUtil"

:: Create a second test file for the encrypted tests (server rejects duplicate names)
echo hello from hpuft encrypted test > %TEST_FILE_ENC%

echo.
echo ══════════════════════════════════════════════
echo  PUSH (encrypted)
echo ══════════════════════════════════════════════
echo [test] Encrypted source checksum:
certutil -hashfile %TEST_FILE_ENC% SHA256 | findstr /v "hash" | findstr /v "CertUtil"
%EXE% push -addr %ADDR% -file %TEST_FILE_ENC% -encrypt
if errorlevel 1 ( echo [test] ENCRYPTED PUSH FAILED & goto :done )

:: Clean recv dir for encrypted pull
if exist %RECV_DIR% rmdir /s /q %RECV_DIR%
mkdir %RECV_DIR%

echo.
echo ══════════════════════════════════════════════
echo  PULL (encrypted)
echo ══════════════════════════════════════════════
%EXE% get -addr %ADDR% -file %TEST_FILE_ENC% -out %RECV_DIR% -encrypt
if errorlevel 1 ( echo [test] ENCRYPTED PULL FAILED & goto :done )

echo.
echo [test] Received file contents (encrypted pull):
type %RECV_DIR%\%TEST_FILE_ENC%

echo [test] Received checksum (encrypted pull):
certutil -hashfile %RECV_DIR%\%TEST_FILE_ENC% SHA256 | findstr /v "hash" | findstr /v "CertUtil"

echo.
echo [test] ALL TESTS PASSED

:done
:: Cleanup
if exist %TEST_FILE% del %TEST_FILE%
if exist %TEST_FILE_ENC% del %TEST_FILE_ENC%
endlocal
