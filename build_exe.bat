@echo off
setlocal

REM Build Windows .exe (wrapper that calls PowerShell script)

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0build_exe.ps1"
if errorlevel 1 exit /b 1

endlocal
