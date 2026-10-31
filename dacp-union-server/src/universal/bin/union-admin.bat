@echo off
call "%~dp0dacp-union-server.bat" -main dacp.unionserver.Admin %*
exit /b %errorlevel%
