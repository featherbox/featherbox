@echo off
setlocal enabledelayedexpansion

echo 🔨 Building FeatherBox for Windows (x86_64-pc-windows-msvc)

where pnpm >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ❌ pnpm is not installed. Please install pnpm first.
    exit /b 1
)

rustup target list --installed | findstr "x86_64-pc-windows-msvc" >nul
if %ERRORLEVEL% neq 0 (
    echo 📦 Adding x86_64-pc-windows-msvc target...
    rustup target add x86_64-pc-windows-msvc
    if %ERRORLEVEL% neq 0 (
        echo ❌ Failed to add Windows target
        exit /b 1
    )
)

echo 🏗️  Building binary...
cargo build --release --target x86_64-pc-windows-msvc
if %ERRORLEVEL% neq 0 (
    echo ❌ Build failed
    exit /b 1
)

echo 📦 Creating distribution package...
if not exist dist mkdir dist
copy target\x86_64-pc-windows-msvc\release\fbox.exe dist\fbox-windows-x86_64.exe
if %ERRORLEVEL% neq 0 (
    echo ❌ Failed to copy binary
    exit /b 1
)

powershell -Command "Compress-Archive -Path 'dist\fbox-windows-x86_64.exe' -DestinationPath 'dist\fbox-windows-x86_64.zip' -Force"
if %ERRORLEVEL% neq 0 (
    echo ❌ Failed to create zip package
    exit /b 1
)

echo ✅ Windows build completed!
echo 📁 Binary: dist\fbox-windows-x86_64.exe
echo 📦 Package: dist\fbox-windows-x86_64.zip

echo.
echo 🔍 Checking dependencies...
where dumpbin >nul 2>&1
if %ERRORLEVEL% equ 0 (
    dumpbin /dependents dist\fbox-windows-x86_64.exe
) else (
    echo ⚠️  dumpbin not available, cannot check dependencies
)