"""
Constants Module for Yad2 Monitor
Centralized configuration and magic numbers
"""

# ============ HTTP & Network ============
DEFAULT_TIMEOUT = 15  # seconds
DEFAULT_MAX_RETRIES = 3
MIN_REQUEST_INTERVAL = 0.5  # seconds between requests

# ============ Scraping Intervals ============
DEFAULT_MIN_INTERVAL_MINUTES = 60
DEFAULT_MAX_INTERVAL_MINUTES = 90
ADAPTIVE_DELAY_MIN_SECONDS = 2
ADAPTIVE_DELAY_MAX_SECONDS = 300

# ============ Database ============
DATABASE_TIMEOUT = 30.0  # seconds
DEFAULT_DATABASE_PATH = "yad2_monitor.db"

# ============ Notifications ============
MIN_MESSAGE_INTERVAL = 0.5  # seconds between Telegram messages
DEFAULT_DAILY_DIGEST_HOUR = 20  # 8 PM
NOTIFICATION_RATE_LIMIT_PER_HOUR = 100
NOTIFICATION_RATE_LIMIT_PER_MINUTE = 20

# ============ Price Change Detection ============
MIN_PRICE_CHANGE_PCT = 1.0  # Minimum 1% change to notify
SIGNIFICANT_PRICE_DROP_PCT = 5.0  # 5% drop is significant
PRICE_DROP_LOOKBACK_DAYS = 7

# ============ Market Analytics ============
DEFAULT_TREND_DAYS = 30
PRICE_HISTORY_MAX_POINTS = 100
TOP_NEIGHBORHOODS_COUNT = 10

# ============ Web Server ============
DEFAULT_WEB_PORT = 5000
DEFAULT_WEB_HOST = "0.0.0.0"
API_RATE_LIMIT_PER_HOUR = 100
API_RATE_LIMIT_PER_MINUTE = 20
DEFAULT_PAGINATION_LIMIT = 100
MAX_PAGINATION_LIMIT = 1000

# ============ Validation Limits ============
MIN_PRICE = 0
MAX_PRICE = 100_000_000
MIN_ROOMS = 0.5
MAX_ROOMS = 20
MIN_SQM = 10
MAX_SQM = 1000
MIN_FLOOR = -5  # Underground parking
MAX_FLOOR = 100

# ============ Telegram Bot ============
TELEGRAM_API_TIMEOUT = 10  # seconds
MAX_TELEGRAM_MESSAGE_LENGTH = 4096
INLINE_BUTTON_CALLBACK_PREFIX_LENGTH = 64

# ============ Search & Filtering ============
SEARCH_RESULTS_PREVIEW_LIMIT = 8
FAVORITES_PREVIEW_LIMIT = 10
MAX_FILTER_NAME_LENGTH = 50
MAX_APARTMENT_TITLE_LENGTH = 200

# ============ File Export ============
TEMP_FILE_PREFIX = 'yad2_export_'
CSV_EXPORT_ENCODING = 'utf-8-sig'  # Excel-friendly UTF-8

# ============ Logging ============
DEFAULT_LOG_LEVEL = "INFO"
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# ============ User Agent Strings ============
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
]

# ============ Status Messages ============
STATUS_ACTIVE = 'active'
STATUS_REMOVED = 'removed'
STATUS_INACTIVE = 'inactive'

# ============ Notification Types ============
NOTIFICATION_TYPE_NEW = 'new'
NOTIFICATION_TYPE_PRICE_DROP = 'price_drop'
NOTIFICATION_TYPE_PRICE_CHANGE = 'price_change'
NOTIFICATION_TYPE_REMOVED = 'removed'
NOTIFICATION_TYPE_BACK_ON_MARKET = 'back_on_market'

# ============ Error Messages ============
ERROR_INVALID_PRICE = "מחיר לא תקין / Invalid price"
ERROR_INVALID_ROOMS = "מספר חדרים לא תקין / Invalid number of rooms"
ERROR_INVALID_SQM = "מ\"ר לא תקין / Invalid square meters"
ERROR_INVALID_PAGINATION = "פרמטרי עימוד לא תקינים / Invalid pagination parameters"
ERROR_NOT_FOUND = "לא נמצא / Not found"
ERROR_UNAUTHORIZED = "אין הרשאה / Unauthorized"
ERROR_RATE_LIMIT = "חרגת ממכסת הבקשות / Rate limit exceeded"
ERROR_INTERNAL = "שגיאת שרת פנימית / Internal server error"

# ============ Success Messages ============
SUCCESS_ADDED_FAVORITE = "✓ נוסף למועדפים"
SUCCESS_REMOVED_FAVORITE = "✓ הוסר מהמועדפים"
SUCCESS_IGNORED = "✓ הדירה תתעלם"
SUCCESS_FILTER_SAVED = "✓ הפילטר נשמר"
SUCCESS_SETTINGS_UPDATED = "✓ ההגדרות עודכנו"

# ============ Dashboard ============
DASHBOARD_REFRESH_INTERVAL_MS = 300000  # 5 minutes
CHART_MAX_DATA_POINTS = 50
DEFAULT_ANALYTICS_DAYS = 7

# ============ Price Distribution Ranges ============
PRICE_RANGES = [
    (0, 3000),
    (3000, 5000),
    (5000, 7000),
    (7000, 10000),
    (10000, 15000),
    (15000, 20000),
    (20000, float('inf'))
]

# ============ Emoji Icons ============
EMOJI_NEW = "🆕"
EMOJI_PRICE_DROP = "📉"
EMOJI_PRICE_UP = "📈"
EMOJI_LOCATION = "📍"
EMOJI_PRICE = "💰"
EMOJI_ROOMS = "🛏️"
EMOJI_SQM = "📏"
EMOJI_FLOOR = "🏢"
EMOJI_FAVORITE = "⭐"
EMOJI_SEARCH = "🔍"
EMOJI_STATS = "📊"
EMOJI_SUCCESS = "✓"
EMOJI_ERROR = "✗"
EMOJI_WARNING = "⚠"
