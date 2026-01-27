# Yad2 Monitor

A professional, multi-user real estate monitoring system for Yad2.co.il with real-time notifications, interactive web dashboard, and comprehensive Telegram bot integration.

## Features

### Security & Reliability
- API key authentication for all endpoints
- XSS protection with HTML escaping
- Input validation with bilingual error messages (Hebrew/English)
- Rate limiting (100 requests/hour, 20/minute)
- Thread-safe database operations
- CORS configuration for production deployment
- Comprehensive error handling with user-friendly messages

### Web Dashboard
- Modern, responsive UI with dark mode support
- Interactive Chart.js visualizations (price distribution, neighborhood trends, market analytics)
- Advanced filtering (price, rooms, square meters, city, neighborhood)
- Real-time search with debouncing and autocomplete
- Filter persistence with localStorage
- Toast notifications for user feedback
- Mobile-first responsive design
- CSV/JSON export functionality

### Multi-User Telegram Bot
- 9 interactive commands:
  - `/start` - Welcome message and registration
  - `/help` - Command reference
  - `/status` - System status and monitoring info
  - `/stats` - Personal statistics and market overview
  - `/favorites` - View and manage favorite apartments
  - `/search [query]` - Search apartments from Telegram
  - `/filter` - Manage search filters
  - `/analytics` - Market insights and trends
  - `/pause` / `/resume` - Control notifications
- Inline keyboard buttons for quick actions (favorite, ignore, open link)
- Per-user preferences and filters
- Multi-user notification broadcasting
- Filter-based notification routing

### Real Estate Monitoring
- Automatic scraping with adaptive intervals (60-90 minutes)
- Price change detection and alerts
- New listing notifications
- Removed listing tracking
- Price history tracking
- Market analytics and trends
- Neighborhood statistics

## Prerequisites

- Python 3.8 or higher
- SQLite (included with Python)
- PostgreSQL (recommended for production)
- Telegram Bot Token (from [@BotFather](https://t.me/botfather))
- Public URL for Telegram webhook (for production deployment)

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/avishaynaim/Myhand.git
cd Myhand
```

### 2. Create Virtual Environment

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Copy the example environment file and configure it:

```bash
cp .env.example .env
```

Edit `.env` with your configuration (see Configuration section below).

### 5. Run the Application

```bash
python app.py
```

The web dashboard will be available at `http://localhost:5000`

## Configuration

### Required Environment Variables

```bash
# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN=your_bot_token_here    # Get from @BotFather
TELEGRAM_CHAT_ID=your_chat_id_here        # Your Telegram chat ID (legacy, optional)

# API Security
API_KEY=your_secret_api_key_here          # Generate: openssl rand -hex 32
```

### Optional Environment Variables

```bash
# Telegram Webhook (required for production)
TELEGRAM_WEBHOOK_URL=https://yourdomain.com

# Yad2 Search URL (can add multiple via API)
YAD2_URL=https://www.yad2.co.il/realestate/rent?topArea=41&area=12&city=8400

# Scraping Intervals (minutes)
MIN_INTERVAL_MINUTES=60
MAX_INTERVAL_MINUTES=90

# Web Dashboard
ENABLE_WEB=true
WEB_PORT=5000

# CORS Configuration
ALLOWED_ORIGINS=*                          # Use specific origins in production

# Proxy Configuration (optional)
PROXY_LIST=ip:port,ip:port:user:pass
HTTP_PROXY=
HTTPS_PROXY=
```

### Getting Your Telegram Bot Token

1. Open Telegram and search for [@BotFather](https://t.me/botfather)
2. Send `/newbot` command
3. Follow the instructions to name your bot
4. Copy the bot token provided
5. Paste it in your `.env` file as `TELEGRAM_BOT_TOKEN`

### Getting Your Chat ID

1. Start a chat with your bot
2. Send any message
3. Visit: `https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates`
4. Find your chat ID in the JSON response
5. Paste it in your `.env` file as `TELEGRAM_CHAT_ID` (optional, for backward compatibility)

## Usage

### Web Dashboard

1. Open `http://localhost:5000` in your browser
2. Use the API key in requests:
   - Header: `X-API-Key: your_api_key`
   - Or URL parameter: `?api_key=your_api_key`

3. Dashboard features:
   - View all apartments with filtering
   - Search by keyword
   - Filter by price, rooms, square meters, location
   - Save and load filter presets
   - Toggle dark mode
   - View price drops and favorites
   - Export data to CSV/JSON
   - View analytics and charts

### Telegram Bot

1. Start the bot: Send `/start` to your bot on Telegram
2. Available commands:
   - `/help` - Get list of all commands
   - `/status` - Check system status
   - `/stats` - View your statistics
   - `/favorites` - Manage favorite apartments
   - `/search תל אביב` - Search for apartments
   - `/filter` - Manage your search filters
   - `/analytics` - View market analytics
   - `/pause` - Pause notifications
   - `/resume` - Resume notifications

3. Notifications:
   - Receive new apartment alerts with inline buttons
   - Get price change notifications
   - Click buttons to favorite, ignore, or open listings

## API Documentation

All API endpoints require authentication via `X-API-Key` header or `api_key` query parameter.

### Apartments

- `GET /api/apartments` - Get all apartments with optional filters
  - Query params: `min_price`, `max_price`, `min_rooms`, `max_rooms`, `city`, `neighborhood`, `limit`, `offset`
- `GET /api/apartments/:id` - Get specific apartment details
- `GET /api/search` - Search apartments
  - Query params: `q` (search query)

### Favorites

- `GET /api/favorites` - Get user favorites
- `POST /api/favorites/:id` - Add to favorites
- `DELETE /api/favorites/:id` - Remove from favorites

### Analytics

- `GET /api/stats` - Get overall statistics
- `GET /api/analytics` - Get market analytics
  - Query params: `days` (default: 7)
- `GET /api/trends` - Get daily trends
  - Query params: `days` (default: 7)
- `GET /api/price-drops` - Get recent price drops
  - Query params: `days` (default: 7), `min_drop_pct` (default: 5)

### Filters

- `GET /api/filters` - Get user filters
- `POST /api/filters` - Create filter
- `DELETE /api/filters/:id` - Delete filter

### Export

- `GET /api/export/csv` - Export apartments to CSV
- `GET /api/export/json` - Export apartments to JSON

### System

- `GET /health` - Health check endpoint (no auth required)
- `POST /telegram/webhook` - Telegram webhook endpoint (no auth required)

### Error Responses

All endpoints return bilingual error messages (Hebrew/English):

```json
{
  "error": "מחיר לא תקין / Invalid price",
  "details": "..."
}
```

HTTP Status Codes:
- `400` - Bad Request (validation error)
- `401` - Unauthorized (missing/invalid API key)
- `404` - Not Found
- `429` - Too Many Requests (rate limit exceeded)
- `500` - Internal Server Error

## Deployment

### Railway Deployment

1. Install Railway CLI:
```bash
npm install -g @railway/cli
```

2. Login to Railway:
```bash
railway login
```

3. Create new project:
```bash
railway init
```

4. Add environment variables:
```bash
railway variables set TELEGRAM_BOT_TOKEN=your_token
railway variables set API_KEY=your_api_key
railway variables set TELEGRAM_WEBHOOK_URL=https://your-app.up.railway.app
```

5. Deploy:
```bash
railway up
```

6. Set Telegram webhook:
```bash
curl -X POST "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/setWebhook" \
  -d "url=https://your-app.up.railway.app/telegram/webhook"
```

### Docker Deployment

1. Build image:
```bash
docker build -t yad2-monitor .
```

2. Run container:
```bash
docker run -d \
  -p 5000:5000 \
  -e TELEGRAM_BOT_TOKEN=your_token \
  -e API_KEY=your_api_key \
  -e TELEGRAM_WEBHOOK_URL=https://your-domain.com \
  yad2-monitor
```

### Environment Configuration for Production

1. Set strong API key:
```bash
openssl rand -hex 32
```

2. Configure allowed origins:
```bash
ALLOWED_ORIGINS=https://yourdomain.com,https://www.yourdomain.com
```

3. Set webhook URL:
```bash
TELEGRAM_WEBHOOK_URL=https://yourdomain.com
```

4. Use PostgreSQL for production:
   - Update database.py to use PostgreSQL connection
   - Set DATABASE_URL environment variable

## Project Structure

```
myhand/
├── app.py                  # Main application entry point
├── web.py                  # Flask web server and API routes
├── database.py             # Database operations and multi-user schema
├── scraper.py              # Yad2 scraping logic
├── notifications.py        # Notification manager
├── telegram_bot.py         # Telegram bot implementation
├── analytics.py            # Market analytics
├── auth.py                 # API authentication
├── validation.py           # Input validation
├── config.py               # Configuration management
├── constants.py            # Centralized constants
├── requirements.txt        # Python dependencies
├── .env.example            # Environment variables template
├── templates/
│   └── dashboard.html      # Dashboard HTML template
└── static/
    ├── css/
    │   └── dashboard.css   # Dashboard styles
    └── js/
        ├── dashboard.js    # Dashboard functionality
        ├── charts.js       # Chart.js visualizations
        └── toast.js        # Toast notifications
```

## Database Schema

### Multi-User Tables

- `telegram_users` - Registered Telegram users
- `user_preferences` - Per-user settings (JSON)
- `user_favorites` - User-specific favorite apartments
- `user_ignored` - User-specific ignored apartments
- `user_filters` - User-specific search filters

### Apartment Tables

- `apartments` - All scraped apartments
- `price_history` - Price change tracking
- `notifications_sent` - Notification history

## Development

### Running Tests

```bash
pytest tests/
```

### Code Quality

```bash
# Type checking
mypy *.py --strict

# Linting
pylint *.py

# Code formatting
black *.py
```

### Adding New Features

1. Create feature branch
2. Implement changes
3. Add tests
4. Update documentation
5. Submit pull request

## Troubleshooting

### Bot not receiving updates

1. Check webhook status:
```bash
curl "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getWebhookInfo"
```

2. Delete webhook (for local development):
```bash
curl "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/deleteWebhook"
```

### Database locked errors

- Ensure thread-local connections are properly implemented
- Check for long-running transactions
- Consider PostgreSQL for production

### Rate limiting issues

- Adjust rate limits in web.py
- Check ALLOWED_ORIGINS configuration
- Verify API key is being sent correctly

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [Flask](https://flask.palletsprojects.com/) - Web framework
- [Chart.js](https://www.chartjs.org/) - Data visualization
- [Telegram Bot API](https://core.telegram.org/bots/api) - Bot integration
- [Yad2.co.il](https://www.yad2.co.il/) - Real estate data source

## Support

For issues and questions:
- Open an issue on GitHub
- Contact: [Your contact information]

## Changelog

### Version 2.0.0 (2026-01-27)
- Added multi-user Telegram bot support
- Implemented API authentication
- Enhanced dashboard with charts and dark mode
- Added comprehensive input validation
- Improved security and thread safety
- Added real-time search and filtering
- Created mobile-responsive design

### Version 1.0.0
- Initial release
- Basic scraping and notifications
- Simple web dashboard
- Single-user Telegram notifications
