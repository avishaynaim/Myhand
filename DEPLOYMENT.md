# Deployment Guide

## Railway Deployment

Railway is a modern platform for deploying applications with zero configuration. Follow these steps to deploy Yad2 Monitor to Railway.

### Prerequisites

1. [Railway Account](https://railway.app/) (free tier available)
2. Railway CLI installed (optional, for CLI deployment)
3. GitHub repository with your code

### Method 1: Deploy via Railway Dashboard (Recommended)

#### Step 1: Create New Project

1. Go to [Railway Dashboard](https://railway.app/dashboard)
2. Click "New Project"
3. Select "Deploy from GitHub repo"
4. Authorize Railway to access your GitHub account
5. Select the `avishaynaim/Myhand` repository

#### Step 2: Configure Environment Variables

In the Railway dashboard, go to your project's Variables tab and add:

**Required Variables:**
```
TELEGRAM_BOT_TOKEN=your_bot_token_from_botfather
API_KEY=your_generated_api_key
```

**Important Variables:**
```
TELEGRAM_WEBHOOK_URL=https://your-app.up.railway.app
TELEGRAM_CHAT_ID=your_telegram_chat_id
```

**Optional Variables:**
```
YAD2_URL=https://www.yad2.co.il/realestate/rent?topArea=41&area=12&city=8400
MIN_INTERVAL_MINUTES=60
MAX_INTERVAL_MINUTES=90
ENABLE_WEB=true
WEB_PORT=5000
ALLOWED_ORIGINS=https://your-app.up.railway.app
```

#### Step 3: Get Your Railway App URL

1. After deployment, Railway will assign you a URL like `https://your-app.up.railway.app`
2. Copy this URL
3. Update the `TELEGRAM_WEBHOOK_URL` variable with this URL

#### Step 4: Set Telegram Webhook

Once your app is deployed, set the Telegram webhook:

```bash
curl -X POST "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/setWebhook" \
  -H "Content-Type: application/json" \
  -d '{"url": "https://your-app.up.railway.app/telegram/webhook"}'
```

Replace:
- `<YOUR_BOT_TOKEN>` with your actual bot token
- `your-app.up.railway.app` with your Railway app URL

#### Step 5: Verify Deployment

1. Check deployment logs in Railway dashboard
2. Visit `https://your-app.up.railway.app/health` to verify the app is running
3. Send `/start` to your Telegram bot to test

### Method 2: Deploy via Railway CLI

#### Step 1: Install Railway CLI

```bash
npm install -g @railway/cli
```

#### Step 2: Login to Railway

```bash
railway login
```

#### Step 3: Initialize Project

```bash
cd Myhand
railway init
```

#### Step 4: Add Environment Variables

```bash
railway variables set TELEGRAM_BOT_TOKEN=your_token_here
railway variables set API_KEY=your_api_key_here
railway variables set TELEGRAM_WEBHOOK_URL=https://your-app.up.railway.app
```

#### Step 5: Deploy

```bash
railway up
```

#### Step 6: Get App URL

```bash
railway domain
```

Copy the URL and update `TELEGRAM_WEBHOOK_URL`:

```bash
railway variables set TELEGRAM_WEBHOOK_URL=https://your-actual-url.up.railway.app
```

#### Step 7: Set Telegram Webhook

```bash
curl -X POST "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/setWebhook" \
  -H "Content-Type: application/json" \
  -d '{"url": "https://your-actual-url.up.railway.app/telegram/webhook"}'
```

### Method 3: Deploy from CLI (One Command)

```bash
railway up
```

This will automatically:
- Detect it's a Python project
- Install dependencies from `requirements.txt`
- Run the start command from `Procfile`

### Generating API Key

Generate a secure API key using OpenSSL:

```bash
openssl rand -hex 32
```

Or Python:

```python
import secrets
print(secrets.token_hex(32))
```

### Database Considerations

**For Production:**

Railway offers a free PostgreSQL database. To use PostgreSQL instead of SQLite:

1. In Railway dashboard, click "New" → "Database" → "PostgreSQL"
2. Railway will add a `DATABASE_URL` environment variable
3. Modify `database.py` to use PostgreSQL connection when `DATABASE_URL` is set

**For Development/Small Scale:**

The default SQLite database works fine for small to medium scale deployments.

### Monitoring & Logs

**View Logs:**
```bash
railway logs
```

**Monitor Resource Usage:**
- Go to Railway dashboard → Your Project → Metrics

### Updating the Deployment

**Via Git:**
1. Push changes to GitHub
2. Railway auto-deploys on every push to `main` branch

**Via CLI:**
```bash
railway up
```

### Troubleshooting

#### App Not Starting

Check logs:
```bash
railway logs
```

Common issues:
- Missing environment variables
- Port configuration (Railway assigns `PORT` env var automatically)
- Database connection errors

#### Telegram Bot Not Responding

1. Verify webhook is set:
```bash
curl "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getWebhookInfo"
```

2. Check webhook URL is correct
3. Verify app is accessible at the webhook URL
4. Check Railway logs for errors

#### Database Locked Errors

If using SQLite with high traffic:
1. Consider switching to PostgreSQL
2. Increase `busy_timeout` in database.py
3. Enable WAL mode (already enabled)

### Custom Domain (Optional)

1. Go to Railway dashboard → Settings → Domains
2. Click "Add Custom Domain"
3. Follow instructions to configure DNS

### Cost Estimates

Railway Free Tier:
- $5 worth of usage per month
- Enough for small projects
- Automatically scales to zero when not in use

Pro Plan:
- $20/month base
- Usage-based pricing beyond included credits

### Security Checklist

- ✅ API key is set and secure (32+ character random string)
- ✅ ALLOWED_ORIGINS is set to specific domain (not `*`)
- ✅ Environment variables are not hardcoded in code
- ✅ `.env` file is in `.gitignore`
- ✅ Telegram webhook uses HTTPS
- ✅ Database file permissions are secure

### Next Steps

1. Set up monitoring/alerting
2. Configure backup strategy for database
3. Set up custom domain
4. Configure rate limiting appropriately
5. Monitor resource usage and optimize as needed

## Alternative Deployments

### Docker Deployment

```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "app.py"]
```

Build and run:
```bash
docker build -t yad2-monitor .
docker run -p 5000:5000 --env-file .env yad2-monitor
```

### Heroku Deployment

1. Create `runtime.txt`:
```
python-3.9.18
```

2. Deploy:
```bash
heroku create yad2-monitor
heroku config:set TELEGRAM_BOT_TOKEN=your_token
heroku config:set API_KEY=your_key
git push heroku main
```

### VPS Deployment

1. SSH into your server
2. Clone repository
3. Install dependencies
4. Set up systemd service
5. Configure nginx reverse proxy
6. Set up SSL with Let's Encrypt

See detailed VPS deployment guide in the README.

---

For issues or questions, open an issue on [GitHub](https://github.com/avishaynaim/Myhand/issues).
