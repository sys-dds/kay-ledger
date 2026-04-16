@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "MAVEN_VERSION=3.9.14"
set "MAVEN_HOME=%USERPROFILE%\.m2\wrapper\dists\apache-maven-%MAVEN_VERSION%"
set "MAVEN_BIN=%MAVEN_HOME%\bin\mvn.cmd"

if not exist "%MAVEN_BIN%" (
  powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Stop'; $version='%MAVEN_VERSION%'; $base=Join-Path $env:USERPROFILE '.m2\wrapper\dists'; $mavenHome=Join-Path $base ('apache-maven-' + $version); $archive=Join-Path $base ('apache-maven-' + $version + '-bin.zip'); New-Item -ItemType Directory -Force -Path $base | Out-Null; if (!(Test-Path $archive)) { Invoke-WebRequest -Uri ('https://archive.apache.org/dist/maven/maven-3/' + $version + '/binaries/apache-maven-' + $version + '-bin.zip') -OutFile $archive }; if (!(Test-Path $mavenHome)) { Expand-Archive -Path $archive -DestinationPath $base -Force }"
  if errorlevel 1 exit /b %errorlevel%
)

"%MAVEN_BIN%" -f "%SCRIPT_DIR%pom.xml" %*
exit /b %errorlevel%
