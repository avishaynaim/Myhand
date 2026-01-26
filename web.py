"""
Web Dashboard & REST API for Yad2 Monitor
Flask-based dashboard with REST endpoints
"""
from flask import Flask, jsonify, request, render_template_string, send_file
from flask_cors import CORS
from datetime import datetime
import os
import json
import logging
from functools import wraps

logger = logging.getLogger(__name__)

# HTML Template for Dashboard
DASHBOARD_HTML = '''
<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Yad2 Monitor Dashboard</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 {
            color: white;
            text-align: center;
            margin-bottom: 30px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .stat-card {
            background: white;
            border-radius: 15px;
            padding: 25px;
            text-align: center;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            transition: transform 0.3s;
        }
        .stat-card:hover { transform: translateY(-5px); }
        .stat-value {
            font-size: 2.5em;
            font-weight: bold;
            color: #667eea;
        }
        .stat-label { color: #666; margin-top: 10px; }
        .card {
            background: white;
            border-radius: 15px;
            padding: 25px;
            margin-bottom: 20px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        }
        .card h2 {
            color: #333;
            margin-bottom: 20px;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }
        .apartment-list { list-style: none; }
        .apartment-item {
            padding: 15px;
            border-bottom: 1px solid #eee;
            display: flex;
            justify-content: space-between;
            align-items: center;
            transition: background 0.2s;
        }
        .apartment-item:hover { background: #f8f9fa; }
        .apartment-item:last-child { border-bottom: none; }
        .apartment-title {
            font-weight: 600;
            color: #333;
            flex: 1;
        }
        .apartment-price {
            color: #28a745;
            font-weight: bold;
            font-size: 1.2em;
        }
        .apartment-location { color: #666; font-size: 0.9em; }
        .btn {
            display: inline-block;
            padding: 8px 16px;
            background: #667eea;
            color: white;
            border-radius: 8px;
            text-decoration: none;
            margin-left: 10px;
            font-size: 0.9em;
            transition: background 0.2s;
        }
        .btn:hover { background: #5a6fd6; }
        .btn-fav { background: #ffc107; color: #333; }
        .btn-fav:hover { background: #e0a800; }
        .filters {
            display: flex;
            gap: 15px;
            flex-wrap: wrap;
            margin-bottom: 20px;
        }
        .filter-group { display: flex; flex-direction: column; }
        .filter-group label { font-size: 0.9em; color: #666; margin-bottom: 5px; }
        .filter-group input, .filter-group select {
            padding: 10px;
            border: 2px solid #ddd;
            border-radius: 8px;
            font-size: 1em;
        }
        .filter-group input:focus, .filter-group select:focus {
            border-color: #667eea;
            outline: none;
        }
        .price-change-down { color: #28a745; }
        .price-change-up { color: #dc3545; }
        .tabs {
            display: flex;
            gap: 10px;
            margin-bottom: 20px;
        }
        .tab {
            padding: 12px 24px;
            background: rgba(255,255,255,0.2);
            color: white;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-size: 1em;
            transition: all 0.2s;
        }
        .tab.active, .tab:hover { background: white; color: #667eea; }
        .hidden { display: none; }
        .loading {
            text-align: center;
            padding: 40px;
            color: #666;
        }
        .empty-state {
            text-align: center;
            padding: 40px;
            color: #666;
        }
        @media (max-width: 768px) {
            .filters { flex-direction: column; }
            .apartment-item { flex-direction: column; align-items: flex-start; gap: 10px; }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🏠 Yad2 Monitor Dashboard</h1>

        <div class="stats-grid" id="stats">
            <div class="stat-card">
                <div class="stat-value" id="total-apartments">-</div>
                <div class="stat-label">דירות פעילות</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="avg-price">-</div>
                <div class="stat-label">מחיר ממוצע</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="new-today">-</div>
                <div class="stat-label">חדשות היום</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="price-changes">-</div>
                <div class="stat-label">שינויי מחיר</div>
            </div>
        </div>

        <div class="tabs">
            <button class="tab active" onclick="showTab('apartments')">דירות</button>
            <button class="tab" onclick="showTab('favorites')">מועדפים</button>
            <button class="tab" onclick="showTab('price-drops')">ירידות מחיר</button>
            <button class="tab" onclick="showTab('analytics')">אנליטיקה</button>
        </div>

        <div id="apartments-tab" class="card">
            <h2>🏠 דירות פעילות</h2>
            <div class="filters">
                <div class="filter-group">
                    <label>מחיר מינימום</label>
                    <input type="number" id="min-price" placeholder="0" onchange="filterApartments()">
                </div>
                <div class="filter-group">
                    <label>מחיר מקסימום</label>
                    <input type="number" id="max-price" placeholder="999999" onchange="filterApartments()">
                </div>
                <div class="filter-group">
                    <label>חדרים (מינימום)</label>
                    <input type="number" id="min-rooms" placeholder="1" onchange="filterApartments()">
                </div>
                <div class="filter-group">
                    <label>מיון</label>
                    <select id="sort-by" onchange="filterApartments()">
                        <option value="date">תאריך</option>
                        <option value="price-asc">מחיר (נמוך לגבוה)</option>
                        <option value="price-desc">מחיר (גבוה לנמוך)</option>
                    </select>
                </div>
            </div>
            <ul class="apartment-list" id="apartment-list">
                <li class="loading">טוען...</li>
            </ul>
        </div>

        <div id="favorites-tab" class="card hidden">
            <h2>⭐ מועדפים</h2>
            <ul class="apartment-list" id="favorites-list">
                <li class="loading">טוען...</li>
            </ul>
        </div>

        <div id="price-drops-tab" class="card hidden">
            <h2>📉 ירידות מחיר אחרונות</h2>
            <ul class="apartment-list" id="price-drops-list">
                <li class="loading">טוען...</li>
            </ul>
        </div>

        <div id="analytics-tab" class="card hidden">
            <h2>📊 אנליטיקה</h2>
            <div id="analytics-content">
                <div class="loading">טוען...</div>
            </div>
        </div>
    </div>

    <script>
        const API_BASE = '/api';
        let allApartments = [];

        async function fetchData(endpoint) {
            try {
                const response = await fetch(`${API_BASE}${endpoint}`);
                return await response.json();
            } catch (error) {
                console.error('Error fetching data:', error);
                return null;
            }
        }

        async function loadStats() {
            const data = await fetchData('/stats');
            if (data) {
                document.getElementById('total-apartments').textContent = data.total_listings || 0;
                document.getElementById('avg-price').textContent =
                    data.avg_price ? `₪${data.avg_price.toLocaleString()}` : '-';
                document.getElementById('new-today').textContent = data.new_this_week || 0;
                document.getElementById('price-changes').textContent = data.price_changes_this_week || 0;
            }
        }

        async function loadApartments() {
            const data = await fetchData('/apartments');
            if (data && data.apartments) {
                allApartments = data.apartments;
                renderApartments(allApartments);
            }
        }

        function renderApartments(apartments) {
            const list = document.getElementById('apartment-list');
            if (!apartments.length) {
                list.innerHTML = '<li class="empty-state">אין דירות להצגה</li>';
                return;
            }
            list.innerHTML = apartments.map(apt => `
                <li class="apartment-item">
                    <div>
                        <div class="apartment-title">${apt.title || 'ללא כותרת'}</div>
                        <div class="apartment-location">${apt.street_address || apt.location || ''}</div>
                    </div>
                    <div>
                        <span class="apartment-price">₪${(apt.price || 0).toLocaleString()}</span>
                        <a href="${apt.link}" target="_blank" class="btn">צפייה</a>
                        <button class="btn btn-fav" onclick="toggleFavorite('${apt.id}')">⭐</button>
                    </div>
                </li>
            `).join('');
        }

        function filterApartments() {
            const minPrice = parseInt(document.getElementById('min-price').value) || 0;
            const maxPrice = parseInt(document.getElementById('max-price').value) || Infinity;
            const minRooms = parseInt(document.getElementById('min-rooms').value) || 0;
            const sortBy = document.getElementById('sort-by').value;

            let filtered = allApartments.filter(apt =>
                apt.price >= minPrice &&
                apt.price <= maxPrice &&
                (apt.rooms || 0) >= minRooms
            );

            if (sortBy === 'price-asc') filtered.sort((a, b) => (a.price || 0) - (b.price || 0));
            else if (sortBy === 'price-desc') filtered.sort((a, b) => (b.price || 0) - (a.price || 0));

            renderApartments(filtered);
        }

        async function loadFavorites() {
            const data = await fetchData('/favorites');
            const list = document.getElementById('favorites-list');
            if (data && data.favorites && data.favorites.length) {
                list.innerHTML = data.favorites.map(apt => `
                    <li class="apartment-item">
                        <div>
                            <div class="apartment-title">${apt.title || 'ללא כותרת'}</div>
                            <div class="apartment-location">${apt.street_address || ''}</div>
                        </div>
                        <div>
                            <span class="apartment-price">₪${(apt.price || 0).toLocaleString()}</span>
                            <a href="${apt.link}" target="_blank" class="btn">צפייה</a>
                        </div>
                    </li>
                `).join('');
            } else {
                list.innerHTML = '<li class="empty-state">אין מועדפים</li>';
            }
        }

        async function loadPriceDrops() {
            const data = await fetchData('/price-drops');
            const list = document.getElementById('price-drops-list');
            if (data && data.drops && data.drops.length) {
                list.innerHTML = data.drops.map(item => `
                    <li class="apartment-item">
                        <div>
                            <div class="apartment-title">${item.title || 'ללא כותרת'}</div>
                            <div class="price-change-down">
                                ₪${item.old_price.toLocaleString()} → ₪${item.new_price.toLocaleString()}
                                (${item.drop_pct}%-)
                            </div>
                        </div>
                        <div>
                            <a href="${item.link}" target="_blank" class="btn">צפייה</a>
                        </div>
                    </li>
                `).join('');
            } else {
                list.innerHTML = '<li class="empty-state">אין ירידות מחיר אחרונות</li>';
            }
        }

        async function loadAnalytics() {
            const data = await fetchData('/analytics');
            const content = document.getElementById('analytics-content');
            if (data) {
                let html = '<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px;">';

                if (data.overall) {
                    html += `
                        <div class="card" style="margin: 0;">
                            <h3>סיכום כללי</h3>
                            <p>סה"כ דירות: ${data.overall.total_listings}</p>
                            <p>מחיר ממוצע: ₪${data.overall.avg_price?.toLocaleString() || '-'}</p>
                            <p>מ"ר ממוצע: ${data.overall.avg_sqm || '-'}</p>
                        </div>
                    `;
                }

                if (data.price_distribution) {
                    html += `
                        <div class="card" style="margin: 0;">
                            <h3>התפלגות מחירים</h3>
                            ${data.price_distribution.map(d =>
                                `<p>${d.range}: ${d.count} דירות</p>`
                            ).join('')}
                        </div>
                    `;
                }

                if (data.top_neighborhoods) {
                    html += `
                        <div class="card" style="margin: 0;">
                            <h3>שכונות מובילות</h3>
                            ${data.top_neighborhoods.slice(0, 5).map(n =>
                                `<p>${n.name}: ${n.count} דירות</p>`
                            ).join('')}
                        </div>
                    `;
                }

                html += '</div>';
                content.innerHTML = html;
            }
        }

        async function toggleFavorite(aptId) {
            await fetch(`${API_BASE}/favorites/${aptId}`, { method: 'POST' });
            loadFavorites();
        }

        function showTab(tabName) {
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('[id$="-tab"]').forEach(t => t.classList.add('hidden'));

            event.target.classList.add('active');
            document.getElementById(`${tabName}-tab`).classList.remove('hidden');

            if (tabName === 'favorites') loadFavorites();
            else if (tabName === 'price-drops') loadPriceDrops();
            else if (tabName === 'analytics') loadAnalytics();
        }

        // Initial load
        loadStats();
        loadApartments();

        // Refresh every 5 minutes
        setInterval(() => {
            loadStats();
            loadApartments();
        }, 300000);
    </script>
</body>
</html>
'''


def create_web_app(database, analytics=None):
    """Create and configure Flask application"""
    app = Flask(__name__)
    CORS(app)

    db = database
    market_analytics = analytics

    # ============ Dashboard Routes ============

    @app.route('/')
    def dashboard():
        """Serve the dashboard HTML"""
        return render_template_string(DASHBOARD_HTML)

    @app.route('/health')
    def health_check():
        """Health check endpoint for monitoring"""
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'database': 'connected' if db else 'not configured'
        })

    # ============ API Routes ============

    @app.route('/api/apartments')
    def get_apartments():
        """Get all apartments with optional filtering"""
        filters = {
            'min_price': request.args.get('min_price', type=int),
            'max_price': request.args.get('max_price', type=int),
            'min_rooms': request.args.get('min_rooms', type=float),
            'max_rooms': request.args.get('max_rooms', type=float),
            'neighborhood': request.args.get('neighborhood'),
            'city': request.args.get('city'),
            'limit': request.args.get('limit', type=int, default=100)
        }
        # Remove None values
        filters = {k: v for k, v in filters.items() if v is not None}

        apartments = db.get_apartments_filtered(filters) if filters else db.get_all_apartments()

        return jsonify({
            'apartments': apartments,
            'total': len(apartments),
            'filters_applied': filters
        })

    @app.route('/api/apartments/<apt_id>')
    def get_apartment(apt_id):
        """Get single apartment by ID"""
        apt = db.get_apartment(apt_id)
        if not apt:
            return jsonify({'error': 'Apartment not found'}), 404

        # Include price history
        price_history = db.get_price_history(apt_id)
        apt['price_history'] = price_history

        # Include comparison if analytics available
        if market_analytics:
            apt['comparison'] = market_analytics.get_comparison(apt_id)

        return jsonify(apt)

    @app.route('/api/stats')
    def get_stats():
        """Get market statistics"""
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

    @app.route('/api/analytics')
    def get_analytics():
        """Get detailed analytics"""
        if not market_analytics:
            return jsonify({'error': 'Analytics not configured'}), 501

        return jsonify(market_analytics.get_market_insights())

    @app.route('/api/trends')
    def get_trends():
        """Get price trends"""
        if not market_analytics:
            return jsonify({'error': 'Analytics not configured'}), 501

        days = request.args.get('days', type=int, default=30)
        group_by = request.args.get('group_by', default='neighborhood')

        return jsonify(market_analytics.get_price_trends(days, group_by))

    @app.route('/api/price-drops')
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
    def get_favorites():
        """Get all favorites"""
        favorites = db.get_favorites()
        return jsonify({
            'favorites': favorites,
            'total': len(favorites)
        })

    @app.route('/api/favorites/<apt_id>', methods=['POST'])
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
    def remove_favorite(apt_id):
        """Remove from favorites"""
        db.remove_favorite(apt_id)
        return jsonify({'status': 'removed'})

    @app.route('/api/ignored', methods=['GET'])
    def get_ignored():
        """Get ignored apartments"""
        ignored = list(db.get_ignored_ids())
        return jsonify({'ignored': ignored})

    @app.route('/api/ignored/<apt_id>', methods=['POST'])
    def add_ignored(apt_id):
        """Add to ignored list"""
        reason = request.json.get('reason') if request.is_json else None
        db.add_ignored(apt_id, reason)
        return jsonify({'status': 'ignored'})

    @app.route('/api/ignored/<apt_id>', methods=['DELETE'])
    def remove_ignored(apt_id):
        """Remove from ignored"""
        db.remove_ignored(apt_id)
        return jsonify({'status': 'removed'})

    @app.route('/api/search-urls', methods=['GET'])
    def get_search_urls():
        """Get all search URLs"""
        urls = db.get_search_urls(active_only=False)
        return jsonify({'urls': urls})

    @app.route('/api/search-urls', methods=['POST'])
    def add_search_url():
        """Add new search URL"""
        data = request.json
        if not data or not data.get('name') or not data.get('url'):
            return jsonify({'error': 'name and url required'}), 400

        url_id = db.add_search_url(data['name'], data['url'])
        return jsonify({'id': url_id, 'status': 'added'})

    @app.route('/api/filters', methods=['GET'])
    def get_filters():
        """Get active filters"""
        filters = db.get_active_filters()
        return jsonify({'filters': filters})

    @app.route('/api/filters', methods=['POST'])
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
    def export_csv():
        """Export apartments to CSV"""
        filepath = f"/tmp/yad2_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        success = db.export_to_csv(filepath)
        if success:
            return send_file(filepath, as_attachment=True, download_name='apartments.csv')
        return jsonify({'error': 'Export failed'}), 500

    @app.route('/api/export/price-history')
    def export_price_history():
        """Export price history to CSV"""
        apt_id = request.args.get('apartment_id')
        filepath = f"/tmp/price_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        success = db.export_price_history_csv(filepath, apt_id)
        if success:
            return send_file(filepath, as_attachment=True, download_name='price_history.csv')
        return jsonify({'error': 'Export failed'}), 500

    @app.route('/api/scrape-stats')
    def get_scrape_stats():
        """Get scraping statistics"""
        hours = request.args.get('hours', type=int, default=24)
        stats = db.get_scrape_stats(hours)
        return jsonify(stats)

    @app.route('/api/time-on-market')
    def get_time_on_market():
        """Get time on market statistics"""
        if not market_analytics:
            return jsonify({'error': 'Analytics not configured'}), 501

        apt_id = request.args.get('apartment_id')
        return jsonify(market_analytics.get_time_on_market(apt_id))

    @app.route('/api/comparison/<apt_id>')
    def get_comparison(apt_id):
        """Compare apartment to market"""
        if not market_analytics:
            return jsonify({'error': 'Analytics not configured'}), 501

        return jsonify(market_analytics.get_comparison(apt_id))

    @app.route('/api/daily-summary')
    def get_daily_summary():
        """Get daily summary"""
        date = request.args.get('date')
        summary = db.get_daily_summary(date)
        return jsonify(summary or {'message': 'No summary available'})

    @app.route('/api/settings', methods=['GET'])
    def get_settings():
        """Get all settings"""
        # Common settings
        keys = ['min_interval', 'max_interval', 'instant_notifications', 'daily_digest_enabled']
        settings = {k: db.get_setting(k) for k in keys}
        return jsonify(settings)

    @app.route('/api/settings', methods=['POST'])
    def update_settings():
        """Update settings"""
        data = request.json
        for key, value in data.items():
            db.set_setting(key, str(value))
        return jsonify({'status': 'updated'})

    return app


def run_web_server(database, analytics=None, host='0.0.0.0', port=5000, debug=False):
    """Run the web server"""
    app = create_web_app(database, analytics)
    logger.info(f"Starting web server on {host}:{port}")
    app.run(host=host, port=port, debug=debug, threaded=True)
