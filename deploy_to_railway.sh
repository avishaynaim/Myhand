#!/bin/bash

echo "======================================"
echo "Yad2 Monitor - Railway Deployment"
echo "======================================"
echo ""

# Step 1: Check if logged in
echo "Step 1: Checking Railway authentication..."
if railway whoami &> /dev/null; then
    echo "✅ Already logged in to Railway"
    railway whoami
else
    echo "⚠️  Not logged in. Opening browser for authentication..."
    railway login
    if [ $? -ne 0 ]; then
        echo "❌ Login failed. Please try again."
        exit 1
    fi
fi

echo ""
echo "======================================"

# Step 2: Initialize project
echo "Step 2: Initializing Railway project..."
if [ -f ".railway/config.json" ]; then
    echo "✅ Railway project already initialized"
else
    echo "Creating new Railway project..."
    railway init
    if [ $? -ne 0 ]; then
        echo "❌ Failed to initialize project"
        exit 1
    fi
fi

echo ""
echo "======================================"

# Step 3: Set environment variables
echo "Step 3: Setting environment variables..."
echo ""
echo "Please provide the following information:"
echo ""

# Get Telegram Bot Token
read -p "Enter TELEGRAM_BOT_TOKEN (from @BotFather): " BOT_TOKEN
if [ -z "$BOT_TOKEN" ]; then
    echo "❌ Bot token is required"
    exit 1
fi

# Generate API Key
echo ""
echo "Generating secure API key..."
API_KEY=$(openssl rand -hex 32)
echo "✅ Generated API_KEY: $API_KEY"
echo "⚠️  SAVE THIS API KEY - you'll need it to access the dashboard!"

# Get Chat ID (optional)
echo ""
read -p "Enter TELEGRAM_CHAT_ID (optional, press Enter to skip): " CHAT_ID

echo ""
echo "Setting environment variables on Railway..."

railway variables set TELEGRAM_BOT_TOKEN="$BOT_TOKEN"
railway variables set API_KEY="$API_KEY"

if [ ! -z "$CHAT_ID" ]; then
    railway variables set TELEGRAM_CHAT_ID="$CHAT_ID"
fi

# Set default variables
railway variables set ENABLE_WEB="true"
railway variables set WEB_PORT="5000"
railway variables set MIN_INTERVAL_MINUTES="60"
railway variables set MAX_INTERVAL_MINUTES="90"

echo "✅ Environment variables set"

echo ""
echo "======================================"

# Step 4: Deploy
echo "Step 4: Deploying to Railway..."
railway up

if [ $? -ne 0 ]; then
    echo "❌ Deployment failed"
    exit 1
fi

echo ""
echo "✅ Deployment initiated!"

echo ""
echo "======================================"

# Step 5: Get domain
echo "Step 5: Setting up domain..."
sleep 5  # Wait a bit for deployment to start

echo "Getting Railway domain..."
DOMAIN=$(railway domain)

if [ -z "$DOMAIN" ]; then
    echo "⚠️  No domain found. Generating one..."
    railway domain
    sleep 3
    DOMAIN=$(railway domain)
fi

echo "✅ Your app URL: https://$DOMAIN"

# Update webhook URL
echo ""
echo "Updating TELEGRAM_WEBHOOK_URL..."
railway variables set TELEGRAM_WEBHOOK_URL="https://$DOMAIN"

echo ""
echo "======================================"

# Step 6: Set Telegram webhook
echo "Step 6: Configuring Telegram webhook..."
sleep 10  # Wait for deployment to be ready

WEBHOOK_URL="https://$DOMAIN/telegram/webhook"
echo "Setting webhook to: $WEBHOOK_URL"

curl -X POST "https://api.telegram.org/bot$BOT_TOKEN/setWebhook" \
  -H "Content-Type: application/json" \
  -d "{\"url\": \"$WEBHOOK_URL\"}"

echo ""
echo ""
echo "======================================"
echo "🎉 Deployment Complete!"
echo "======================================"
echo ""
echo "📱 App URL: https://$DOMAIN"
echo "🔐 API Key: $API_KEY"
echo "💬 Telegram Bot: Send /start to your bot"
echo "🏥 Health Check: https://$DOMAIN/health"
echo "📊 Dashboard: https://$DOMAIN/ (requires API key)"
echo ""
echo "📝 Next Steps:"
echo "1. Test your bot by sending /start on Telegram"
echo "2. Access dashboard at https://$DOMAIN/"
echo "3. View logs with: railway logs"
echo "4. Monitor at: railway open"
echo ""
echo "⚠️  IMPORTANT: Save your API key somewhere safe!"
echo "   API Key: $API_KEY"
echo ""
