@echo off
call "%~dp0dacp-server.bat" -main dacp.Admin %*
exit /b %errorlevel%
