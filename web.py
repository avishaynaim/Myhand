"""
Web Dashboard & REST API for Yad2 Monitor
Flask-based dashboard with REST endpoints
"""
from flask import Flask, jsonify, request, render_template, render_template_string, send_file
from flask_cors import CORS
from datetime import datetime
import os
import json
import logging
import tempfile
from functools import wraps

logger = logging.getLogger(__name__)

# Embedded dashboard HTML - inline to avoid import issues
EMBEDDED_DASHBOARD_HTML = '''<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Yad2 Monitor Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
    <style>
        :root { --primary: #667eea; --bg: #f8f9fa; --card: #ffffff; --text: #333333; --border: #dee2e6; --shadow: 0 2px 8px rgba(0,0,0,0.1); }
        [data-theme="dark"] { --bg: #1a1a2e; --card: #0f3460; --text: #e9ecef; --border: #495057; }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: Arial, sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; padding: 20px; transition: all 0.3s; }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { text-align: center; color: var(--primary); margin-bottom: 30px; }
        .nav { display: flex; justify-content: center; gap: 15px; margin-bottom: 30px; flex-wrap: wrap; }
        .nav a { padding: 10px 20px; background: var(--card); border: 2px solid var(--primary); border-radius: 8px; color: var(--primary); text-decoration: none; font-weight: 600; transition: all 0.3s; }
        .nav a:hover { background: var(--primary); color: white; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .stat { background: var(--card); padding: 25px; border-radius: 12px; box-shadow: var(--shadow); text-align: center; }
        .stat-value { font-size: 2.5em; font-weight: bold; color: var(--primary); }
        .stat-label { color: #6c757d; margin-top: 10px; }
        .card { background: var(--card); padding: 30px; border-radius: 12px; box-shadow: var(--shadow); margin-bottom: 20px; }
        .tabs { display: flex; gap: 10px; margin-bottom: 20px; }
        .tab { padding: 12px 24px; background: var(--card); border: 2px solid var(--border); border-radius: 8px; cursor: pointer; font-weight: 600; }
        .tab.active { background: var(--primary); color: white; border-color: var(--primary); }
        .hidden { display: none !important; }
        .theme-btn { position: fixed; bottom: 30px; left: 30px; width: 60px; height: 60px; border-radius: 50%; background: var(--primary); color: white; border: none; font-size: 1.5em; cursor: pointer; box-shadow: var(--shadow); }
        .apartment { background: var(--bg); padding: 20px; margin: 15px 0; border-radius: 8px; border: 2px solid var(--border); }
        .apartment:hover { border-color: var(--primary); }
        .apartment h3 { color: var(--primary); margin-bottom: 10px; }
        .apartment-details { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin: 10px 0; }
        .detail { font-size: 0.9em; }
        .detail strong { color: var(--primary); }
        @media (max-width: 768px) { .stats { grid-template-columns: 1fr 1fr; } }
    </style>
</head>
<body>
    <div class="container">
        <h1>🏠 Yad2 Monitor Dashboard</h1>
        <div class="nav">
            <a href="/endpoints">📋 API Endpoints</a>
            <a href="/health">💚 Health</a>
            <a href="/api/apartments">🏢 Apartments</a>
            <a href="/api/stats">📊 Stats</a>
        </div>
        <div class="stats">
            <div class="stat"><div class="stat-value" id="total">-</div><div class="stat-label">דירות פעילות</div></div>
            <div class="stat"><div class="stat-value" id="avg-price">-</div><div class="stat-label">מחיר ממוצע</div></div>
            <div class="stat"><div class="stat-value" id="new-today">-</div><div class="stat-label">חדשות (48 שעות)</div></div>
            <div class="stat"><div class="stat-value" id="price-drops">-</div><div class="stat-label">ירידות מחיר</div></div>
        </div>
        <div class="tabs">
            <button class="tab active" onclick="showTab('apartments')">דירות</button>
            <button class="tab" onclick="showTab('analytics')">אנליטיקה</button>
        </div>
        <div id="apartments-tab" class="card">
            <h2>🏠 דירות אחרונות (50 אחרונות)</h2>
            <div id="apartments-list">טוען...</div>
        </div>
        <div id="analytics-tab" class="card hidden">
            <h2>📊 אנליטיקה</h2>
            <canvas id="chart" style="max-height: 400px;"></canvas>
        </div>
    </div>
    <button class="theme-btn" onclick="toggleTheme()" title="החלף ערכת נושא">🌙</button>
    <script>
        let apartments=[];function toggleTheme(){const html=document.documentElement;const theme=html.getAttribute('data-theme')==='dark'?'light':'dark';html.setAttribute('data-theme',theme);localStorage.setItem('theme',theme);event.target.textContent=theme==='dark'?'☀️':'🌙';}function showTab(tab){document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));document.querySelectorAll('[id$="-tab"]').forEach(t=>t.classList.add('hidden'));event.target.classList.add('active');document.getElementById(tab+'-tab').classList.remove('hidden');if(tab==='analytics')loadChart();}async function loadStats(){try{const res=await fetch('/health');const data=await res.json();document.getElementById('total').textContent=data.listings?.total_active||0;document.getElementById('avg-price').textContent=(data.listings?.avg_price||0).toLocaleString()+' ₪';document.getElementById('new-today').textContent=data.today?.new_apartments||0;document.getElementById('price-drops').textContent=data.today?.price_drops||0;}catch(e){console.error(e);}}async function loadApartments(){try{const res=await fetch('/api/apartments?limit=50');apartments=await res.json();renderApartments();}catch(e){document.getElementById('apartments-list').innerHTML='<p>Error loading apartments. Make sure to include API key in headers.</p>';}}function renderApartments(){const list=document.getElementById('apartments-list');if(!apartments.length){list.innerHTML='<p>No apartments found</p>';return;}list.innerHTML=apartments.map(apt=>`<div class="apartment"><h3>${apt.title||'N/A'}</h3><div class="apartment-details"><div class="detail"><strong>💰 מחיר:</strong> ${(apt.price||0).toLocaleString()} ₪</div><div class="detail"><strong>🛏️ חדרים:</strong> ${apt.rooms||'N/A'}</div><div class="detail"><strong>📐 מ"ר:</strong> ${apt.square_meters||'N/A'}</div><div class="detail"><strong>📍 עיר:</strong> ${apt.city||'N/A'}</div><div class="detail"><strong>🏘️ שכונה:</strong> ${apt.neighborhood||'N/A'}</div><div class="detail"><strong>📅 תאריך:</strong> ${new Date(apt.first_seen).toLocaleDateString('he-IL')}</div></div>${apt.link?`<a href="${apt.link}" target="_blank" style="color: var(--primary);">🔗 לינק למודעה</a>`:''}</div>`).join('');}async function loadChart(){if(!apartments.length)return;const ctx=document.getElementById('chart');const prices=apartments.map(a=>a.price).filter(p=>p>0);const avg=prices.reduce((a,b)=>a+b,0)/prices.length;new Chart(ctx,{type:'bar',data:{labels:['Min','Avg','Max'],datasets:[{label:'Price (₪)',data:[Math.min(...prices),avg,Math.max(...prices)],backgroundColor:['#10b981','#667eea','#ef4444']}]},options:{responsive:true,maintainAspectRatio:false}});}const theme=localStorage.getItem('theme')||'light';document.documentElement.setAttribute('data-theme',theme);document.querySelector('.theme-btn').textContent=theme==='dark'?'☀️':'🌙';loadStats();loadApartments();setInterval(loadStats,60000);
    </script>
</body>
</html>'''

EMBEDDED_DASHBOARD_AVAILABLE = True
logger.info("Embedded dashboard HTML defined successfully")

# Import authentication decorator
try:
    from auth import require_api_key
except ImportError:
    logger.warning("auth.py not found - API endpoints will be unprotected!")
    # Fallback no-op decorator if auth module is missing
    def require_api_key(f):
        return f

# Import validation utilities
try:
    from validation import (
        ValidationError,
        validate_apartment_id,
        validate_price_range,
        validate_pagination,
        validate_hours_param,
        validate_days_param,
        sanitize_search_query
    )
except ImportError:
    logger.warning("validation.py not found - input validation disabled!")

    class ValidationError(Exception):
        pass

    # Fallback no-op validators
    def validate_apartment_id(x): return x
    def validate_price_range(x, y): return x, y
    def validate_pagination(x, y): return x or 0, y or 100
    def validate_hours_param(x, d, m): return x or d
    def validate_days_param(x, d, m): return x or d
    def sanitize_search_query(x): return x

# Dashboard HTML moved to templates/dashboard.html
# CSS moved to static/css/dashboard.css
# JavaScript moved to static/js/dashboard.js


def create_web_app(database, analytics=None, telegram_bot=None):
    """Create and configure Flask application"""
    # Get absolute paths for templates and static directories
    base_dir = os.path.abspath(os.path.dirname(__file__))
    template_dir = os.path.join(base_dir, 'templates')
    static_dir = os.path.join(base_dir, 'static')

    app = Flask(__name__,
                template_folder=template_dir,
                static_folder=static_dir)

    # Log template and static paths for debugging
    logger.info(f"Base directory: {base_dir}")
    logger.info(f"Template directory: {template_dir} (exists: {os.path.exists(template_dir)})")
    logger.info(f"Static directory: {static_dir} (exists: {os.path.exists(static_dir)})")

    if os.path.exists(template_dir):
        templates = os.listdir(template_dir)
        logger.info(f"Templates found: {templates}")
    else:
        logger.error(f"Template directory not found! Looking in: {template_dir}")

    # Configure CORS securely
    allowed_origins = os.environ.get('ALLOWED_ORIGINS', '*').split(',')
    CORS(app, origins=allowed_origins, supports_credentials=True)

    # Configure rate limiting
    try:
        from flask_limiter import Limiter
        from flask_limiter.util import get_remote_address

        limiter = Limiter(
            app=app,
            key_func=get_remote_address,
            default_limits=["100 per hour", "20 per minute"],
            storage_uri="memory://",
            strategy="fixed-window"
        )
        logger.info("Rate limiting configured: 100/hour, 20/minute")
    except ImportError:
        logger.warning("flask-limiter not installed - rate limiting disabled")
        limiter = None

    db = database
    market_analytics = analytics
    app_start_time = datetime.now()

    # ============ Error Handlers ============

    @app.errorhandler(400)
    def bad_request(e):
        """Handle 400 Bad Request errors"""
        return jsonify({
            'error': 'בקשה לא תקינה / Bad Request',
            'message': str(e.description) if hasattr(e, 'description') else 'Invalid request'
        }), 400

    @app.errorhandler(401)
    def unauthorized(e):
        """Handle 401 Unauthorized errors"""
        return jsonify({
            'error': 'אין הרשאה / Unauthorized',
            'message': 'Authentication required'
        }), 401

    @app.errorhandler(404)
    def not_found(e):
        """Handle 404 Not Found errors"""
        return jsonify({
            'error': 'לא נמצא / Not Found',
            'message': 'Resource not found'
        }), 404

    @app.errorhandler(429)
    def rate_limit_exceeded(e):
        """Handle 429 Too Many Requests errors"""
        return jsonify({
            'error': 'יותר מדי בקשות / Too Many Requests',
            'message': 'Rate limit exceeded. Please try again later.'
        }), 429

    @app.errorhandler(500)
    def internal_error(e):
        """Handle 500 Internal Server Error"""
        logger.error(f"Internal server error: {e}", exc_info=True)
        return jsonify({
            'error': 'שגיאה פנימית / Internal Server Error',
            'message': 'An unexpected error occurred'
        }), 500

    @app.errorhandler(Exception)
    def handle_exception(e):
        """Handle uncaught exceptions"""
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        return jsonify({
            'error': 'שגיאה לא צפויה / Unexpected Error',
            'message': 'An unexpected error occurred'
        }), 500

    # ============ Dashboard Routes ============

    @app.route('/')
    def dashboard():
        """Serve the dashboard HTML"""
        # Priority 1: Use embedded dashboard (always works)
        if EMBEDDED_DASHBOARD_AVAILABLE:
            logger.info("Serving embedded dashboard")
            return render_template_string(EMBEDDED_DASHBOARD_HTML)

        # Priority 2: Try template file
        try:
            logger.info("Attempting to load dashboard.html template")
            return render_template('dashboard.html')
        except Exception as e:
            logger.warning(f"Could not load dashboard.html template: {e}")

        # Priority 3: Basic fallback
        logger.warning("Serving basic fallback HTML")
        return render_template_string('''
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Yad2 Monitor - API</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; background: #f5f5f5; }
        h1 { color: #333; }
        .card { background: white; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .endpoint { padding: 10px; margin: 10px 0; background: #f9f9f9; border-left: 4px solid #667eea; }
        a { color: #667eea; text-decoration: none; }
        a:hover { text-decoration: underline; }
        code { background: #e8e8e8; padding: 2px 6px; border-radius: 3px; }
    </style>
</head>
<body>
    <h1>🏠 Yad2 Monitor API</h1>

    <div class="card">
        <h2>System Status</h2>
        <p>✅ Application is running</p>
        <p><a href="/health">View Health Status →</a></p>
    </div>

    <div class="card">
        <h2>Quick Links</h2>
        <div class="endpoint">
            <strong><a href="/health">GET /health</a></strong><br>
            System health check and statistics
        </div>
        <div class="endpoint">
            <strong><a href="/api/apartments">GET /api/apartments</a></strong><br>
            List all apartments (requires API key header: <code>X-API-Key</code>)
        </div>
        <div class="endpoint">
            <strong><a href="/api/stats">GET /api/stats</a></strong><br>
            Get market statistics (requires API key)
        </div>
        <div class="endpoint">
            <strong><a href="/endpoints">GET /endpoints</a></strong><br>
            View all available API endpoints
        </div>
    </div>

    <div class="card">
        <h2>Authentication</h2>
        <p>Most endpoints require an API key. Include it in your requests:</p>
        <p><code>X-API-Key: your_api_key_here</code></p>
        <p>Or as a query parameter: <code>?api_key=your_api_key_here</code></p>
    </div>

    <div class="card">
        <h2>Documentation</h2>
        <p>For full documentation, visit the <a href="https://github.com/avishaynaim/Myhand">GitHub repository</a></p>
    </div>
</body>
</html>
''')

    @app.route('/endpoints')
    def list_endpoints():
        """List all available API endpoints"""
        endpoints_html = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Yad2 Monitor - API Endpoints</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 900px; margin: 0 auto; }
        h1 { color: white; text-align: center; margin-bottom: 10px; }
        .subtitle { color: rgba(255,255,255,0.8); text-align: center; margin-bottom: 30px; }
        .card {
            background: white;
            border-radius: 15px;
            padding: 25px;
            margin-bottom: 20px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        }
        .card h2 { color: #333; margin-bottom: 15px; border-bottom: 2px solid #667eea; padding-bottom: 10px; }
        .endpoint-list { list-style: none; }
        .endpoint-item {
            padding: 12px 15px;
            border-bottom: 1px solid #eee;
            display: flex;
            align-items: center;
            gap: 15px;
        }
        .endpoint-item:last-child { border-bottom: none; }
        .endpoint-item:hover { background: #f8f9fa; }
        .method {
            font-size: 0.75em;
            font-weight: bold;
            padding: 4px 8px;
            border-radius: 4px;
            min-width: 50px;
            text-align: center;
        }
        .get { background: #61affe; color: white; }
        .post { background: #49cc90; color: white; }
        .delete { background: #f93e3e; color: white; }
        .endpoint-link {
            color: #667eea;
            text-decoration: none;
            font-family: monospace;
            font-size: 1.1em;
        }
        .endpoint-link:hover { text-decoration: underline; }
        .endpoint-desc { color: #666; font-size: 0.9em; margin-left: auto; }
        .back-link {
            display: inline-block;
            margin-bottom: 20px;
            color: white;
            text-decoration: none;
            font-size: 1.1em;
        }
        .back-link:hover { text-decoration: underline; }
    </style>
</head>
<body>
    <div class="container">
        <a href="/" class="back-link">← Back to Dashboard</a>
        <h1>API Endpoints</h1>
        <p class="subtitle">Click any endpoint to open it</p>

        <div class="card">
            <h2>Pages</h2>
            <ul class="endpoint-list">
                <li class="endpoint-item">
                    <span class="method get">GET</span>
                    <a href="/" class="endpoint-link">/</a>
                    <span class="endpoint-desc">Main Dashboard</span>
                </li>
                <li class="endpoint-item">
                    <span class="method get">GET</span>
                    <a href="/health" class="endpoint-link">/health</a>
                    <span class="endpoint-desc">System Health & Stats</span>
                </li>
                <li class="endpoint-item">
                    <span class="method get">GET</span>
                    <a href="/endpoints" class="endpoint-link">/endpoints</a>
                    <span class="endpoint-desc">This page</span>
                </li>
            </ul>
        </div>

        <div class="card">
            <h2>Apartments</h2>
            <ul class="endpoint-list">
                <li class="endpoint-item">
                    <span class="method get">GET</span>
                    <a href="/api/apartments" class="endpoint-link">/api/apartments</a>
                    <span class="endpoint-desc">All apartments (filterable)</span>
                </li>
                <li class="endpoint-item">
                    <span class="method get">GET</span>
                    <a href="/api/favorites" class="endpoint-link">/api/favorites</a>
                    <span class="endpoint-desc">Favorite apartments</span>
                </li>
                <li class="endpoint-item">
                    <span class="method get">GET</span>
                    <a href="/api/ignored" class="endpoint-link">/api/ignored</a>
                    <span class="endpoint-desc">Ignored apartments</span>
                </li>
            </ul>
        </div>

        <div class="card">
            <h2>Analytics & Stats</h2>
            <ul class="endpoint-list">
                <li class="endpoint-item">
                    <span class="method get">GET</span>
                    <a href="/api/stats" class="endpoint-link">/api/stats</a>
                    <span class="endpoint-desc">Market statistics</span>
                </li>
                <li class="endpoint-item">
                    <span class="method get">GET</span>
                    <a href="/api/analytics" class="endpoint-link">/api/analytics</a>
                    <span class="endpoint-desc">Detailed analytics</span>
                </li>
                <li class="endpoint-item">
                    <span class="method get">GET</span>
                    <a href="/api/trends" class="endpoint-link">/api/trends</a>
                    <span class="endpoint-desc">Price trends</span>
                </li>
                <li class="endpoint-item">
                    <span class="method get">GET</span>
                    <a href="/api/price-drops" class="endpoint-link">/api/price-drops</a>
                    <span class="endpoint-desc">Recent price drops</span>
                </li>
                <li class="endpoint-item">
                    <span class="method get">GET</span>
                    <a href="/api/daily-summary" class="endpoint-link">/api/daily-summary</a>
                    <span class="endpoint-desc">Today's summary</span>
                </li>
                <li class="endpoint-item">
                    <span class="method get">GET</span>
                    <a href="/api/scrape-stats" class="endpoint-link">/api/scrape-stats</a>
                    <span class="endpoint-desc">Scraping statistics</span>
                </li>
                <li class="endpoint-item">
                    <span class="method get">GET</span>
                    <a href="/api/time-on-market" class="endpoint-link">/api/time-on-market</a>
                    <span class="endpoint-desc">Time on market analysis</span>
                </li>
            </ul>
        </div>

        <div class="card">
            <h2>Configuration</h2>
            <ul class="endpoint-list">
                <li class="endpoint-item">
                    <span class="method get">GET</span>
                    <a href="/api/search-urls" class="endpoint-link">/api/search-urls</a>
                    <span class="endpoint-desc">Active search URLs</span>
                </li>
                <li class="endpoint-item">
                    <span class="method get">GET</span>
                    <a href="/api/filters" class="endpoint-link">/api/filters</a>
                    <span class="endpoint-desc">Active filters</span>
                </li>
                <li class="endpoint-item">
                    <span class="method get">GET</span>
                    <a href="/api/settings" class="endpoint-link">/api/settings</a>
                    <span class="endpoint-desc">App settings</span>
                </li>
            </ul>
        </div>

        <div class="card">
            <h2>Export</h2>
            <ul class="endpoint-list">
                <li class="endpoint-item">
                    <span class="method get">GET</span>
                    <a href="/api/export/csv" class="endpoint-link">/api/export/csv</a>
                    <span class="endpoint-desc">Download CSV</span>
                </li>
                <li class="endpoint-item">
                    <span class="method get">GET</span>
                    <a href="/api/export/price-history" class="endpoint-link">/api/export/price-history</a>
                    <span class="endpoint-desc">Price history CSV</span>
                </li>
            </ul>
        </div>
    </div>
</body>
</html>
'''
        return render_template_string(endpoints_html)

    @app.route('/health')
    def health_check():
        """Health check endpoint with detailed system status"""
        now = datetime.now()
        uptime = now - app_start_time
        uptime_str = f"{uptime.days}d {uptime.seconds // 3600}h {(uptime.seconds % 3600) // 60}m"

        # Get basic stats
        apartments = db.get_all_apartments() if db else []
        prices = [a['price'] for a in apartments if a.get('price')]

        # Calculate apartments from last 2 days (48 hours)
        # Using data_updated_at which is the update timestamp from Yad2 website (not DB timestamp)
        from datetime import timedelta
        two_days_ago_timestamp = int((now - timedelta(days=2)).timestamp())
        new_apartments_last_2_days = 0

        if apartments:
            for apt in apartments:
                data_updated_at = apt.get('data_updated_at')  # Unix timestamp from website HTML
                if data_updated_at:
                    try:
                        # data_updated_at is a Unix timestamp (integer)
                        if isinstance(data_updated_at, (int, float)) and data_updated_at >= two_days_ago_timestamp:
                            new_apartments_last_2_days += 1
                    except (ValueError, AttributeError, TypeError):
                        pass

        # Get daily summary for other stats
        daily_summary = db.get_daily_summary() if db else None

        # Get scrape stats
        scrape_stats = db.get_scrape_stats(hours=24) if db else {}

        # Get search URLs count
        search_urls = db.get_search_urls() if db else []

        # Get favorites count
        favorites = db.get_favorites() if db else []

        return jsonify({
            'status': 'healthy',
            'timestamp': now.isoformat(),
            'uptime': uptime_str,
            'uptime_seconds': int(uptime.total_seconds()),
            'database': 'connected' if db else 'not configured',
            'listings': {
                'total_active': len(apartments),
                'avg_price': sum(prices) // len(prices) if prices else 0,
                'min_price': min(prices) if prices else 0,
                'max_price': max(prices) if prices else 0,
                'favorites': len(favorites)
            },
            'today': {
                'new_apartments': new_apartments_last_2_days,  # Last 2 days instead of today
                'price_drops': daily_summary.get('price_drops', 0) if daily_summary else 0,
                'price_increases': daily_summary.get('price_increases', 0) if daily_summary else 0,
                'removed': daily_summary.get('removed', 0) if daily_summary else 0
            },
            'scraping': {
                'search_urls_active': len(search_urls),
                'last_24h': scrape_stats
            }
        })

    # ============ API Routes ============

    @app.route('/api/apartments')
    @require_api_key
    def get_apartments():
        """Get all apartments with optional filtering"""
        try:
            # Get and validate parameters
            min_price = request.args.get('min_price', type=int)
            max_price = request.args.get('max_price', type=int)
            min_rooms = request.args.get('min_rooms', type=float)
            max_rooms = request.args.get('max_rooms', type=float)
            limit = request.args.get('limit', type=int, default=100)

            # Validate price range
            min_price, max_price = validate_price_range(min_price, max_price)

            # Validate pagination
            offset, limit = validate_pagination(None, limit)

            filters = {
                'min_price': min_price,
                'max_price': max_price,
                'min_rooms': min_rooms,
                'max_rooms': max_rooms,
                'neighborhood': request.args.get('neighborhood'),
                'city': request.args.get('city'),
                'limit': limit
            }
            # Remove None values
            filters = {k: v for k, v in filters.items() if v is not None}

            apartments = db.get_apartments_filtered(filters) if filters else db.get_all_apartments()

            return jsonify({
                'apartments': apartments,
                'total': len(apartments),
                'filters_applied': filters
            })

        except ValidationError as e:
            return jsonify({'error': str(e)}), 400
        except Exception as e:
            logger.error(f"Error in get_apartments: {e}", exc_info=True)
            return jsonify({'error': 'Failed to fetch apartments'}), 500

    @app.route('/api/apartments/<apt_id>')
    @require_api_key
    def get_apartment(apt_id):
        """Get single apartment by ID"""
        try:
            # Validate apartment ID
            apt_id = validate_apartment_id(apt_id)

            apt = db.get_apartment(apt_id)
            if not apt:
                return jsonify({'error': 'דירה לא נמצאה / Apartment not found'}), 404

            # Include price history
            price_history = db.get_price_history(apt_id)
            apt['price_history'] = price_history

            # Include comparison if analytics available
            if market_analytics:
                try:
                    apt['comparison'] = market_analytics.get_comparison(apt_id)
                except Exception as e:
                    logger.warning(f"Failed to get comparison for {apt_id}: {e}")
                    apt['comparison'] = None

            return jsonify(apt)

        except ValidationError as e:
            return jsonify({'error': str(e)}), 400
        except Exception as e:
            logger.error(f"Error in get_apartment: {e}", exc_info=True)
            return jsonify({'error': 'Failed to fetch apartment details'}), 500

    @app.route('/api/stats')
    @require_api_key
    def get_stats():
        """Get market statistics"""
        try:
            if market_analytics:
                return jsonify(market_analytics.get_market_insights())

            # Basic stats without analytics module
            apartments = db.get_all_apartments()
            prices = [a['price'] for a in apartments if a.get('price')]

            return jsonify({
                'total_listings': len(apartments),
                'avg_price': sum(prices) // len(prices) if prices else 0,
                'min_price': min(prices) if prices else 0,
                'max_price': max(prices) if prices else 0
            })

        except Exception as e:
            logger.error(f"Error in get_stats: {e}", exc_info=True)
            return jsonify({'error': 'Failed to fetch statistics'}), 500

    @app.route('/api/analytics')
    @require_api_key
    def get_analytics():
        """Get detailed analytics"""
        if not market_analytics:
            return jsonify({'error': 'Analytics not configured'}), 501

        return jsonify(market_analytics.get_market_insights())

    @app.route('/api/trends')
    @require_api_key
    def get_trends():
        """Get price trends or daily statistics"""
        if not market_analytics:
            return jsonify({'error': 'Analytics not configured'}), 501

        days = request.args.get('days', type=int, default=30)
        trend_type = request.args.get('type', default='price')

        # Return daily statistics for charts if type=daily
        if trend_type == 'daily':
            return jsonify(market_analytics.get_daily_statistics(days))

        # Otherwise return price trends by group
        group_by = request.args.get('group_by', default='neighborhood')
        return jsonify(market_analytics.get_price_trends(days, group_by))

    @app.route('/api/price-drops')
    @require_api_key
    def get_price_drops():
        """Get recent price drops"""
        if market_analytics:
            min_drop = request.args.get('min_drop', type=float, default=3.0)
            drops = market_analytics.get_price_drop_alerts(min_drop)
            return jsonify({'drops': drops})

        # Without analytics, get from price history
        changes = db.get_price_changes(days=7)
        drops = [c for c in changes if c.get('new_price', 0) < c.get('old_price', 0)]
        return jsonify({'drops': drops})

    @app.route('/api/favorites', methods=['GET'])
    @require_api_key
    def get_favorites():
        """Get all favorites"""
        favorites = db.get_favorites()
        return jsonify({
            'favorites': favorites,
            'total': len(favorites)
        })

    @app.route('/api/favorites/<apt_id>', methods=['POST'])
    @require_api_key
    def toggle_favorite(apt_id):
        """Toggle favorite status"""
        if db.is_favorite(apt_id):
            db.remove_favorite(apt_id)
            return jsonify({'status': 'removed'})
        else:
            notes = request.json.get('notes') if request.is_json else None
            db.add_favorite(apt_id, notes)
            return jsonify({'status': 'added'})

    @app.route('/api/favorites/<apt_id>', methods=['DELETE'])
    @require_api_key
    def remove_favorite(apt_id):
        """Remove from favorites"""
        db.remove_favorite(apt_id)
        return jsonify({'status': 'removed'})

    @app.route('/api/ignored', methods=['GET'])
    @require_api_key
    def get_ignored():
        """Get ignored apartments"""
        ignored = list(db.get_ignored_ids())
        return jsonify({'ignored': ignored})

    @app.route('/api/ignored/<apt_id>', methods=['POST'])
    @require_api_key
    def add_ignored(apt_id):
        """Add to ignored list"""
        reason = request.json.get('reason') if request.is_json else None
        db.add_ignored(apt_id, reason)
        return jsonify({'status': 'ignored'})

    @app.route('/api/ignored/<apt_id>', methods=['DELETE'])
    @require_api_key
    def remove_ignored(apt_id):
        """Remove from ignored"""
        db.remove_ignored(apt_id)
        return jsonify({'status': 'removed'})

    @app.route('/api/search-urls', methods=['GET'])
    @require_api_key
    def get_search_urls():
        """Get all search URLs"""
        urls = db.get_search_urls(active_only=False)
        return jsonify({'urls': urls})

    @app.route('/api/search-urls', methods=['POST'])
    @require_api_key
    def add_search_url():
        """Add new search URL"""
        data = request.json
        if not data or not data.get('name') or not data.get('url'):
            return jsonify({'error': 'name and url required'}), 400

        url_id = db.add_search_url(data['name'], data['url'])
        return jsonify({'id': url_id, 'status': 'added'})

    @app.route('/api/filters', methods=['GET'])
    @require_api_key
    def get_filters():
        """Get active filters"""
        filters = db.get_active_filters()
        return jsonify({'filters': filters})

    @app.route('/api/filters', methods=['POST'])
    @require_api_key
    def add_filter():
        """Add new filter"""
        data = request.json
        if not data or not data.get('name') or not data.get('filter_type'):
            return jsonify({'error': 'name and filter_type required'}), 400

        filter_id = db.add_filter(
            data['name'],
            data['filter_type'],
            data.get('min_value'),
            data.get('max_value'),
            data.get('text_value')
        )
        return jsonify({'id': filter_id, 'status': 'added'})

    @app.route('/api/export/csv')
    @require_api_key
    def export_csv():
        """Export apartments to CSV"""
        # Use temporary file that works on all platforms
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', prefix='yad2_export_')
        filepath = temp_file.name
        temp_file.close()

        try:
            success = db.export_to_csv(filepath)
            if success:
                return send_file(filepath, as_attachment=True, download_name='apartments.csv')
            return jsonify({'error': 'Export failed'}), 500
        finally:
            # Clean up temporary file after sending
            try:
                if os.path.exists(filepath):
                    os.unlink(filepath)
            except Exception as e:
                logger.warning(f"Failed to clean up temp file {filepath}: {e}")

    @app.route('/api/export/price-history')
    @require_api_key
    def export_price_history():
        """Export price history to CSV"""
        apt_id = request.args.get('apartment_id')

        # Use temporary file that works on all platforms
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', prefix='price_history_')
        filepath = temp_file.name
        temp_file.close()

        try:
            success = db.export_price_history_csv(filepath, apt_id)
            if success:
                return send_file(filepath, as_attachment=True, download_name='price_history.csv')
            return jsonify({'error': 'Export failed'}), 500
        finally:
            # Clean up temporary file after sending
            try:
                if os.path.exists(filepath):
                    os.unlink(filepath)
            except Exception as e:
                logger.warning(f"Failed to clean up temp file {filepath}: {e}")

    @app.route('/api/scrape-stats')
    @require_api_key
    def get_scrape_stats():
        """Get scraping statistics"""
        hours = request.args.get('hours', type=int, default=24)
        stats = db.get_scrape_stats(hours)
        return jsonify(stats)

    @app.route('/api/time-on-market')
    @require_api_key
    def get_time_on_market():
        """Get time on market statistics"""
        if not market_analytics:
            return jsonify({'error': 'Analytics not configured'}), 501

        apt_id = request.args.get('apartment_id')
        return jsonify(market_analytics.get_time_on_market(apt_id))

    @app.route('/api/comparison/<apt_id>')
    @require_api_key
    def get_comparison(apt_id):
        """Compare apartment to market"""
        if not market_analytics:
            return jsonify({'error': 'Analytics not configured'}), 501

        return jsonify(market_analytics.get_comparison(apt_id))

    @app.route('/api/daily-summary')
    @require_api_key
    def get_daily_summary():
        """Get daily summary"""
        date = request.args.get('date')
        summary = db.get_daily_summary(date)
        return jsonify(summary or {'message': 'No summary available'})

    @app.route('/api/settings', methods=['GET'])
    @require_api_key
    def get_settings():
        """Get all settings"""
        # Common settings
        keys = ['min_interval', 'max_interval', 'instant_notifications', 'daily_digest_enabled']
        settings = {k: db.get_setting(k) for k in keys}
        return jsonify(settings)

    @app.route('/api/settings', methods=['POST'])
    @require_api_key
    def update_settings():
        """Update settings"""
        data = request.json
        for key, value in data.items():
            db.set_setting(key, str(value))
        return jsonify({'status': 'updated'})

    # ============ Telegram Webhook ============

    @app.route('/telegram/webhook', methods=['POST'])
    def telegram_webhook():
        """Handle Telegram webhook updates"""
        if not telegram_bot:
            logger.warning("Telegram webhook called but bot not configured")
            return jsonify({'error': 'Telegram bot not configured'}), 501

        try:
            update = request.json
            if not update:
                return jsonify({'error': 'No data received'}), 400

            logger.info(f"Received Telegram update: {update.get('update_id', 'unknown')}")
            result = telegram_bot.handle_webhook(update)

            return jsonify(result), 200

        except Exception as e:
            logger.error(f"Error in telegram webhook: {e}", exc_info=True)
            return jsonify({'error': 'Internal server error'}), 500

    return app


def run_web_server(database, analytics=None, telegram_bot=None, host='0.0.0.0', port=5000, debug=False):
    """Run the web server"""
    app = create_web_app(database, analytics, telegram_bot)
    logger.info(f"Starting web server on {host}:{port}")
    app.run(host=host, port=port, debug=debug, threaded=True)
