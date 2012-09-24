@echo off
::---------------------------------------------
::- Author: Ulrich Palha
::- Date: 11/14/2011
::- http://www.ulrichpalha.com
::---------------------------------------------

if "%OS%" == "Windows_NT" setlocal

set "CURRENT_DIR=%cd%"
cd ..
set "APPLICATION_SERVICE_HOME=%cd%"
echo %APPLICATION_SERVICE_HOME%
cd "%CURRENT_DIR%"

set SERVICE_NAME=HydraService

if /i %PROCESSOR_ARCHITECTURE% == x86 (
set EXECUTABLE_NAME=%SERVICE_NAME%.exe
) else (
if /i %PROCESSOR_ARCHITECTURE% == AMD64 (
set EXECUTABLE_NAME=%SERVICE_NAME%_amd64.exe
) else (
set EXECUTABLE_NAME=%SERVICE_NAME%_ia64.exe
)
)

set EXECUTABLE=%APPLICATION_SERVICE_HOME%\bin\%EXECUTABLE_NAME%

set CG_START_CLASS=com.piusvelte.hydra.HydraService
set CG_STOP_CLASS=%CG_START_CLASS%

set CG_START_METHOD=start
set CG_STOP_METHOD=stop

set CG_START_PARAMS=start;properties=%APPLICATION_SERVICE_HOME%\hydra.properties;log=%APPLICATION_SERVICE_HOME%\logs\hydra.log
set CG_STOP_PARAMS=stop

set CG_PATH_TO_JAR_CONTAINING_SERVICE=%APPLICATION_SERVICE_HOME%\bin\Hydra.jar

set CG_STARTUP_TYPE=auto

set CG_PATH_TO_JVM=%JAVA_HOME%\jre\bin\server\jvm.dll

set PR_DESCRIPTION=Hydra Service
set PR_INSTALL=%EXECUTABLE%
set PR_LOGPATH=%APPLICATION_SERVICE_HOME%\logs
set PR_CLASSPATH=%APPLICATION_SERVICE_HOME%;%CG_PATH_TO_JAR_CONTAINING_SERVICE%
set PR_DISPLAYNAME=Hydra Service

if "%1" = "" goto displayUsage
if /i %1 == install goto install
if /i %1 == remove goto remove

:displayUsage
echo Usage: service.bat install/remove [service_name]
goto end

:remove
"%EXECUTABLE% //DS//%SERVICE_NAME%
echo The service '%SERVICE_NAME%' has been removed
goto end

:install
echo Installing service '%SERVICE_NAME%' ...
echo.

set EXECUTE_STRING= %EXECUTABLE% //IS//%SERVICE_NAME%  --Startup %CG_STARTUP_TYPE%  --StartClass %CG_START_CLASS% --StopClass %CG_STOP_CLASS%
call:executeAndPrint %EXECUTE_STRING%

set EXECUTE_STRING= "%EXECUTABLE%" //US//%SERVICE_NAME% --StartMode jvm --StopMode jvm --Jvm %CG_PATH_TO_JVM%
call:executeAndPrint %EXECUTE_STRING%

set EXECUTE_STRING= "%EXECUTABLE%" //US//%SERVICE_NAME% --StartMethod %CG_START_METHOD% --StopMethod  %CG_STOP_METHOD% 
call:executeAndPrint %EXECUTE_STRING%

set EXECUTE_STRING= "%EXECUTABLE%" //US//%SERVICE_NAME% --StartParams %CG_START_PARAMS% --StopParams %CG_STOP_PARAMS% 
call:executeAndPrint %EXECUTE_STRING%

set EXECUTE_STRING= "%EXECUTABLE%" //US//%SERVICE_NAME% ++JvmOptions "-Djava.io.tmpdir=%APPLICATION_SERVICE_HOME%\temp;" --JvmMs 128 --JvmMx 512
call:executeAndPrint %EXECUTE_STRING%

echo. 
echo The service '%SERVICE_NAME%' has been installed.

goto end

::--------
::- Functions
::-------
:executeAndPrint
%*
echo %*

goto:eof

:end
