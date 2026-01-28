"""
Embedded dashboard HTML - simplified version that works without external files
"""

def get_dashboard_html():
    return '''<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Yad2 Monitor Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
    <style>
        :root {
            --primary: #667eea;
            --bg: #f8f9fa;
            --card: #ffffff;
            --text: #333333;
            --border: #dee2e6;
            --shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        [data-theme="dark"] {
            --bg: #1a1a2e;
            --card: #0f3460;
            --text: #e9ecef;
            --border: #495057;
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: Arial, sans-serif;
            background: var(--bg);
            color: var(--text);
            min-height: 100vh;
            padding: 20px;
            transition: all 0.3s;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { text-align: center; color: var(--primary); margin-bottom: 30px; }
        .nav { display: flex; justify-content: center; gap: 15px; margin-bottom: 30px; flex-wrap: wrap; }
        .nav a {
            padding: 10px 20px;
            background: var(--card);
            border: 2px solid var(--primary);
            border-radius: 8px;
            color: var(--primary);
            text-decoration: none;
            font-weight: 600;
            transition: all 0.3s;
        }
        .nav a:hover { background: var(--primary); color: white; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .stat {
            background: var(--card);
            padding: 25px;
            border-radius: 12px;
            box-shadow: var(--shadow);
            text-align: center;
        }
        .stat-value { font-size: 2.5em; font-weight: bold; color: var(--primary); }
        .stat-label { color: #6c757d; margin-top: 10px; }
        .card {
            background: var(--card);
            padding: 30px;
            border-radius: 12px;
            box-shadow: var(--shadow);
            margin-bottom: 20px;
        }
        .tabs { display: flex; gap: 10px; margin-bottom: 20px; }
        .tab {
            padding: 12px 24px;
            background: var(--card);
            border: 2px solid var(--border);
            border-radius: 8px;
            cursor: pointer;
            font-weight: 600;
        }
        .tab.active { background: var(--primary); color: white; border-color: var(--primary); }
        .hidden { display: none !important; }
        .theme-btn {
            position: fixed;
            bottom: 30px;
            left: 30px;
            width: 60px;
            height: 60px;
            border-radius: 50%;
            background: var(--primary);
            color: white;
            border: none;
            font-size: 1.5em;
            cursor: pointer;
            box-shadow: var(--shadow);
        }
        .apartment { background: var(--bg); padding: 20px; margin: 15px 0; border-radius: 8px; border: 2px solid var(--border); }
        .apartment:hover { border-color: var(--primary); }
        .apartment h3 { color: var(--primary); margin-bottom: 10px; }
        .apartment-details { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin: 10px 0; }
        .detail { font-size: 0.9em; }
        .detail strong { color: var(--primary); }
        @media (max-width: 768px) {
            .stats { grid-template-columns: 1fr 1fr; }
        }
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
            <div class="stat">
                <div class="stat-value" id="total">-</div>
                <div class="stat-label">דירות פעילות</div>
            </div>
            <div class="stat">
                <div class="stat-value" id="avg-price">-</div>
                <div class="stat-label">מחיר ממוצע</div>
            </div>
            <div class="stat">
                <div class="stat-value" id="new-today">-</div>
                <div class="stat-label">חדשות היום</div>
            </div>
            <div class="stat">
                <div class="stat-value" id="price-drops">-</div>
                <div class="stat-label">ירידות מחיר</div>
            </div>
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
        let apartments = [];

        function toggleTheme() {
            const html = document.documentElement;
            const theme = html.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
            html.setAttribute('data-theme', theme);
            localStorage.setItem('theme', theme);
            event.target.textContent = theme === 'dark' ? '☀️' : '🌙';
        }

        function showTab(tab) {
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('[id$="-tab"]').forEach(t => t.classList.add('hidden'));
            event.target.classList.add('active');
            document.getElementById(tab + '-tab').classList.remove('hidden');
            if (tab === 'analytics') loadChart();
        }

        async function loadStats() {
            try {
                const res = await fetch('/health');
                const data = await res.json();
                document.getElementById('total').textContent = data.listings?.total_active || 0;
                document.getElementById('avg-price').textContent = (data.listings?.avg_price || 0).toLocaleString() + ' ₪';
                document.getElementById('new-today').textContent = data.today?.new_apartments || 0;
                document.getElementById('price-drops').textContent = data.today?.price_drops || 0;
            } catch (e) { console.error(e); }
        }

        async function loadApartments() {
            try {
                const res = await fetch('/api/apartments?limit=50');
                apartments = await res.json();
                renderApartments();
            } catch (e) {
                document.getElementById('apartments-list').innerHTML = '<p>Error loading apartments. Make sure to include API key in headers.</p>';
            }
        }

        function renderApartments() {
            const list = document.getElementById('apartments-list');
            if (!apartments.length) {
                list.innerHTML = '<p>No apartments found</p>';
                return;
            }
            list.innerHTML = apartments.map(apt => `
                <div class="apartment">
                    <h3>${apt.title || 'N/A'}</h3>
                    <div class="apartment-details">
                        <div class="detail"><strong>💰 מחיר:</strong> ${(apt.price || 0).toLocaleString()} ₪</div>
                        <div class="detail"><strong>🛏️ חדרים:</strong> ${apt.rooms || 'N/A'}</div>
                        <div class="detail"><strong>📐 מ"ר:</strong> ${apt.square_meters || 'N/A'}</div>
                        <div class="detail"><strong>📍 עיר:</strong> ${apt.city || 'N/A'}</div>
                        <div class="detail"><strong>🏘️ שכונה:</strong> ${apt.neighborhood || 'N/A'}</div>
                        <div class="detail"><strong>📅 תאריך:</strong> ${new Date(apt.first_seen).toLocaleDateString('he-IL')}</div>
                    </div>
                    ${apt.link ? `<a href="${apt.link}" target="_blank" style="color: var(--primary);">🔗 לינק למודעה</a>` : ''}
                </div>
            `).join('');
        }

        async function loadChart() {
            if (!apartments.length) return;
            const ctx = document.getElementById('chart');
            const prices = apartments.map(a => a.price).filter(p => p > 0);
            const avg = prices.reduce((a, b) => a + b, 0) / prices.length;

            new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: ['Min', 'Avg', 'Max'],
                    datasets: [{
                        label: 'Price (₪)',
                        data: [Math.min(...prices), avg, Math.max(...prices)],
                        backgroundColor: ['#10b981', '#667eea', '#ef4444']
                    }]
                },
                options: { responsive: true, maintainAspectRatio: false }
            });
        }

        const theme = localStorage.getItem('theme') || 'light';
        document.documentElement.setAttribute('data-theme', theme);
        document.querySelector('.theme-btn').textContent = theme === 'dark' ? '☀️' : '🌙';

        loadStats();
        loadApartments();
        setInterval(loadStats, 60000);
    </script>
</body>
</html>'''
