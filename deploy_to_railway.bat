@echo off
setlocal enabledelayedexpansion

echo ======================================
echo Yad2 Monitor - Railway Deployment
echo ======================================
echo.

REM Step 1: Check if logged in
echo Step 1: Checking Railway authentication...
railway whoami >nul 2>&1
if %errorlevel% equ 0 (
    echo [32m✓ Already logged in to Railway[0m
    railway whoami
) else (
    echo [33m! Not logged in. Opening browser for authentication...[0m
    railway login
    if %errorlevel% neq 0 (
        echo [31mX Login failed. Please try again.[0m
        exit /b 1
    )
)

echo.
echo ======================================

REM Step 2: Initialize project
echo Step 2: Initializing Railway project...
if exist ".railway\config.json" (
    echo [32m✓ Railway project already initialized[0m
) else (
    echo Creating new Railway project...
    railway init
    if %errorlevel% neq 0 (
        echo [31mX Failed to initialize project[0m
        exit /b 1
    )
)

echo.
echo ======================================

REM Step 3: Set environment variables
echo Step 3: Setting environment variables...
echo.
echo Please provide the following information:
echo.

REM Get Telegram Bot Token
set /p BOT_TOKEN="Enter TELEGRAM_BOT_TOKEN (from @BotFather): "
if "!BOT_TOKEN!"=="" (
    echo [31mX Bot token is required[0m
    exit /b 1
)

REM Generate API Key
echo.
echo Generating secure API key...
for /f "delims=" %%i in ('openssl rand -hex 32') do set API_KEY=%%i
echo [32m✓ Generated API_KEY: !API_KEY![0m
echo [33m! SAVE THIS API KEY - you'll need it to access the dashboard![0m

REM Get Chat ID (optional)
echo.
set /p CHAT_ID="Enter TELEGRAM_CHAT_ID (optional, press Enter to skip): "

echo.
echo Setting environment variables on Railway...

railway variables set TELEGRAM_BOT_TOKEN="!BOT_TOKEN!"
railway variables set API_KEY="!API_KEY!"

if not "!CHAT_ID!"=="" (
    railway variables set TELEGRAM_CHAT_ID="!CHAT_ID!"
)

REM Set default variables
railway variables set ENABLE_WEB="true"
railway variables set WEB_PORT="5000"
railway variables set MIN_INTERVAL_MINUTES="60"
railway variables set MAX_INTERVAL_MINUTES="90"

echo [32m✓ Environment variables set[0m

echo.
echo ======================================

REM Step 4: Deploy
echo Step 4: Deploying to Railway...
railway up

if %errorlevel% neq 0 (
    echo [31mX Deployment failed[0m
    exit /b 1
)

echo.
echo [32m✓ Deployment initiated![0m

echo.
echo ======================================

REM Step 5: Get domain
echo Step 5: Setting up domain...
timeout /t 5 /nobreak >nul

echo Getting Railway domain...
for /f "delims=" %%i in ('railway domain') do set DOMAIN=%%i

if "!DOMAIN!"=="" (
    echo [33m! No domain found. Generating one...[0m
    railway domain
    timeout /t 3 /nobreak >nul
    for /f "delims=" %%i in ('railway domain') do set DOMAIN=%%i
)

echo [32m✓ Your app URL: https://!DOMAIN![0m

REM Update webhook URL
echo.
echo Updating TELEGRAM_WEBHOOK_URL...
railway variables set TELEGRAM_WEBHOOK_URL="https://!DOMAIN!"

echo.
echo ======================================

REM Step 6: Set Telegram webhook
echo Step 6: Configuring Telegram webhook...
timeout /t 10 /nobreak >nul

set WEBHOOK_URL=https://!DOMAIN!/telegram/webhook
echo Setting webhook to: !WEBHOOK_URL!

curl -X POST "https://api.telegram.org/bot!BOT_TOKEN!/setWebhook" -H "Content-Type: application/json" -d "{\"url\": \"!WEBHOOK_URL!\"}"

echo.
echo.
echo ======================================
echo [32m🎉 Deployment Complete![0m
echo ======================================
echo.
echo 📱 App URL: https://!DOMAIN!
echo 🔐 API Key: !API_KEY!
echo 💬 Telegram Bot: Send /start to your bot
echo 🏥 Health Check: https://!DOMAIN!/health
echo 📊 Dashboard: https://!DOMAIN!/ (requires API key)
echo.
echo 📝 Next Steps:
echo 1. Test your bot by sending /start on Telegram
echo 2. Access dashboard at https://!DOMAIN!/
echo 3. View logs with: railway logs
echo 4. Monitor at: railway open
echo.
echo [33m! IMPORTANT: Save your API key somewhere safe![0m
echo    API Key: !API_KEY!
echo.

REM Save credentials to file
echo RAILWAY_APP_URL=https://!DOMAIN! > railway_credentials.txt
echo API_KEY=!API_KEY! >> railway_credentials.txt
echo TELEGRAM_BOT_TOKEN=!BOT_TOKEN! >> railway_credentials.txt
echo.
echo [32m✓ Credentials saved to railway_credentials.txt[0m

pause
