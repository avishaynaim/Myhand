"""
SQLite Database Module for Yad2 Monitor
Handles persistent storage for apartments, price history, settings, favorites
"""
import sqlite3
import json
import os
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_path: str = "yad2_monitor.db"):
        self.db_path = db_path
        self._write_lock = threading.Lock()
        self._init_wal_mode()
        self.init_database()

    def _init_wal_mode(self):
        """Enable WAL mode for better concurrent access"""
        conn = sqlite3.connect(self.db_path)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=30000')  # 30 second timeout
        conn.close()

    @contextmanager
    def get_connection(self):
        with self._write_lock:
            conn = sqlite3.connect(
                self.db_path,
                timeout=30.0,  # Wait up to 30 seconds for lock
                check_same_thread=False  # Allow access from different threads
            )
            conn.row_factory = sqlite3.Row
            conn.execute('PRAGMA busy_timeout=30000')
            try:
                yield conn
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                conn.close()

    def init_database(self):
        """Initialize all database tables"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Apartments table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS apartments (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    price INTEGER,
                    price_text TEXT,
                    location TEXT,
                    street_address TEXT,
                    item_info TEXT,
                    link TEXT,
                    image_url TEXT,
                    rooms REAL,
                    sqm INTEGER,
                    floor INTEGER,
                    neighborhood TEXT,
                    city TEXT,
                    data_updated_at INTEGER,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active INTEGER DEFAULT 1,
                    raw_data TEXT
                )
            ''')

            # Price history table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS price_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    apartment_id TEXT NOT NULL,
                    price INTEGER NOT NULL,
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (apartment_id) REFERENCES apartments(id)
                )
            ''')

            # Search URLs table (multiple searches)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS search_urls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_scraped TIMESTAMP
                )
            ''')

            # Favorites table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS favorites (
                    apartment_id TEXT PRIMARY KEY,
                    notes TEXT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (apartment_id) REFERENCES apartments(id)
                )
            ''')

            # Ignored apartments table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ignored (
                    apartment_id TEXT PRIMARY KEY,
                    reason TEXT,
                    ignored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (apartment_id) REFERENCES apartments(id)
                )
            ''')

            # User filters table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS filters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    filter_type TEXT NOT NULL,
                    min_value REAL,
                    max_value REAL,
                    text_value TEXT,
                    is_active INTEGER DEFAULT 1
                )
            ''')

            # Settings table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Scrape logs table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scrape_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Daily summaries table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS daily_summaries (
                    date TEXT PRIMARY KEY,
                    new_apartments INTEGER DEFAULT 0,
                    price_drops INTEGER DEFAULT 0,
                    price_increases INTEGER DEFAULT 0,
                    removed INTEGER DEFAULT 0,
                    avg_price INTEGER,
                    summary_sent INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Notifications queue
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS notification_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    notification_type TEXT NOT NULL,
                    apartment_id TEXT,
                    message TEXT NOT NULL,
                    priority INTEGER DEFAULT 0,
                    scheduled_for TIMESTAMP,
                    sent_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_apartments_price ON apartments(price)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_apartments_location ON apartments(location)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_apartments_last_seen ON apartments(last_seen)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_history_apt ON price_history(apartment_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_history_date ON price_history(recorded_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_scrape_logs_type ON scrape_logs(event_type)')

            logger.info(f"Database initialized at {self.db_path}")

    # ============ Apartment Methods ============

    def upsert_apartment(self, apt: Dict) -> Tuple[str, bool]:
        """Insert or update apartment. Returns (apt_id, is_new)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Check if exists
            cursor.execute('SELECT id, price FROM apartments WHERE id = ?', (apt['id'],))
            existing = cursor.fetchone()

            is_new = existing is None
            price_changed = False

            if existing and existing['price'] != apt.get('price'):
                price_changed = True

            cursor.execute('''
                INSERT INTO apartments (id, title, price, price_text, location, street_address,
                    item_info, link, image_url, rooms, sqm, floor, neighborhood, city,
                    data_updated_at, last_seen, is_active, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                ON CONFLICT(id) DO UPDATE SET
                    title = excluded.title,
                    price = excluded.price,
                    price_text = excluded.price_text,
                    location = excluded.location,
                    street_address = excluded.street_address,
                    item_info = excluded.item_info,
                    link = excluded.link,
                    image_url = excluded.image_url,
                    rooms = excluded.rooms,
                    sqm = excluded.sqm,
                    floor = excluded.floor,
                    neighborhood = excluded.neighborhood,
                    city = excluded.city,
                    data_updated_at = excluded.data_updated_at,
                    last_seen = excluded.last_seen,
                    is_active = 1,
                    raw_data = excluded.raw_data
            ''', (
                apt['id'], apt.get('title'), apt.get('price'), apt.get('price_text'),
                apt.get('location'), apt.get('street_address'), apt.get('item_info'),
                apt.get('link'), apt.get('image_url'), apt.get('rooms'), apt.get('sqm'),
                apt.get('floor'), apt.get('neighborhood'), apt.get('city'),
                apt.get('data_updated_at'), datetime.now().isoformat(),
                json.dumps(apt, ensure_ascii=False)
            ))

            # Record price if changed or new (inline to avoid nested connection)
            if is_new or price_changed:
                if apt.get('price'):
                    cursor.execute(
                        'INSERT INTO price_history (apartment_id, price) VALUES (?, ?)',
                        (apt['id'], apt['price'])
                    )

            return apt['id'], is_new

    def get_apartment(self, apt_id: str) -> Optional[Dict]:
        """Get single apartment by ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM apartments WHERE id = ?', (apt_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_all_apartments(self, active_only: bool = True) -> List[Dict]:
        """Get all apartments"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if active_only:
                cursor.execute('SELECT * FROM apartments WHERE is_active = 1 ORDER BY last_seen DESC')
            else:
                cursor.execute('SELECT * FROM apartments ORDER BY last_seen DESC')
            return [dict(row) for row in cursor.fetchall()]

    def get_apartments_filtered(self, filters: Dict) -> List[Dict]:
        """Get apartments with filters applied"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            query = 'SELECT * FROM apartments WHERE is_active = 1'
            params = []

            if filters.get('min_price'):
                query += ' AND price >= ?'
                params.append(filters['min_price'])
            if filters.get('max_price'):
                query += ' AND price <= ?'
                params.append(filters['max_price'])
            if filters.get('min_rooms'):
                query += ' AND rooms >= ?'
                params.append(filters['min_rooms'])
            if filters.get('max_rooms'):
                query += ' AND rooms <= ?'
                params.append(filters['max_rooms'])
            if filters.get('min_sqm'):
                query += ' AND sqm >= ?'
                params.append(filters['min_sqm'])
            if filters.get('neighborhood'):
                query += ' AND neighborhood LIKE ?'
                params.append(f"%{filters['neighborhood']}%")
            if filters.get('city'):
                query += ' AND city LIKE ?'
                params.append(f"%{filters['city']}%")

            query += ' ORDER BY last_seen DESC'

            if filters.get('limit'):
                query += ' LIMIT ?'
                params.append(filters['limit'])

            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def mark_apartments_inactive(self, active_ids: set):
        """Mark apartments not in active_ids as inactive"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Get current active apartments
            cursor.execute('SELECT id FROM apartments WHERE is_active = 1')
            all_active = {row['id'] for row in cursor.fetchall()}

            # Mark missing ones as inactive
            to_deactivate = all_active - active_ids
            if to_deactivate:
                placeholders = ','.join('?' * len(to_deactivate))
                cursor.execute(f'UPDATE apartments SET is_active = 0 WHERE id IN ({placeholders})',
                              list(to_deactivate))
                logger.info(f"Marked {len(to_deactivate)} apartments as inactive")

            return list(to_deactivate)

    # ============ Price History Methods ============

    def add_price_history(self, apt_id: str, price: int):
        """Add price history entry"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'INSERT INTO price_history (apartment_id, price) VALUES (?, ?)',
                (apt_id, price)
            )

    def get_price_history(self, apt_id: str, limit: int = 50) -> List[Dict]:
        """Get price history for apartment"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT price, recorded_at FROM price_history
                WHERE apartment_id = ?
                ORDER BY recorded_at DESC
                LIMIT ?
            ''', (apt_id, limit))
            return [dict(row) for row in cursor.fetchall()]

    def get_price_changes(self, days: int = 7) -> List[Dict]:
        """Get recent price changes"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cutoff = (datetime.now() - timedelta(days=days)).isoformat()

            cursor.execute('''
                SELECT a.id, a.title, a.link,
                       ph1.price as old_price, ph2.price as new_price,
                       ph2.recorded_at
                FROM apartments a
                JOIN price_history ph1 ON a.id = ph1.apartment_id
                JOIN price_history ph2 ON a.id = ph2.apartment_id
                WHERE ph2.recorded_at > ?
                AND ph1.id = (
                    SELECT id FROM price_history
                    WHERE apartment_id = a.id AND recorded_at < ph2.recorded_at
                    ORDER BY recorded_at DESC LIMIT 1
                )
                AND ph1.price != ph2.price
                ORDER BY ph2.recorded_at DESC
            ''', (cutoff,))
            return [dict(row) for row in cursor.fetchall()]

    # ============ Favorites & Ignored ============

    def add_favorite(self, apt_id: str, notes: str = None):
        """Add apartment to favorites"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'INSERT OR REPLACE INTO favorites (apartment_id, notes) VALUES (?, ?)',
                (apt_id, notes)
            )

    def remove_favorite(self, apt_id: str):
        """Remove from favorites"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM favorites WHERE apartment_id = ?', (apt_id,))

    def get_favorites(self) -> List[Dict]:
        """Get all favorites with apartment details"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT a.*, f.notes, f.added_at as favorited_at
                FROM apartments a
                JOIN favorites f ON a.id = f.apartment_id
                ORDER BY f.added_at DESC
            ''')
            return [dict(row) for row in cursor.fetchall()]

    def is_favorite(self, apt_id: str) -> bool:
        """Check if apartment is favorited"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM favorites WHERE apartment_id = ?', (apt_id,))
            return cursor.fetchone() is not None

    def add_ignored(self, apt_id: str, reason: str = None):
        """Add apartment to ignored list"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'INSERT OR REPLACE INTO ignored (apartment_id, reason) VALUES (?, ?)',
                (apt_id, reason)
            )

    def remove_ignored(self, apt_id: str):
        """Remove from ignored"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM ignored WHERE apartment_id = ?', (apt_id,))

    def get_ignored_ids(self) -> set:
        """Get set of ignored apartment IDs"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT apartment_id FROM ignored')
            return {row['apartment_id'] for row in cursor.fetchall()}

    # ============ Search URLs ============

    def add_search_url(self, name: str, url: str) -> int:
        """Add a search URL to monitor"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'INSERT INTO search_urls (name, url) VALUES (?, ?)',
                (name, url)
            )
            return cursor.lastrowid

    def get_search_urls(self, active_only: bool = True) -> List[Dict]:
        """Get all search URLs"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if active_only:
                cursor.execute('SELECT * FROM search_urls WHERE is_active = 1')
            else:
                cursor.execute('SELECT * FROM search_urls')
            return [dict(row) for row in cursor.fetchall()]

    def update_search_url_scraped(self, url_id: int):
        """Update last scraped time"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE search_urls SET last_scraped = ? WHERE id = ?',
                (datetime.now().isoformat(), url_id)
            )

    # ============ Filters ============

    def add_filter(self, name: str, filter_type: str, min_val=None, max_val=None, text_val=None):
        """Add a notification filter"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO filters (name, filter_type, min_value, max_value, text_value)
                VALUES (?, ?, ?, ?, ?)
            ''', (name, filter_type, min_val, max_val, text_val))
            return cursor.lastrowid

    def get_active_filters(self) -> List[Dict]:
        """Get all active filters"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM filters WHERE is_active = 1')
            return [dict(row) for row in cursor.fetchall()]

    def apartment_passes_filters(self, apt: Dict) -> bool:
        """Check if apartment passes all active filters"""
        filters = self.get_active_filters()

        for f in filters:
            if f['filter_type'] == 'price':
                if f['min_value'] and apt.get('price', 0) < f['min_value']:
                    return False
                if f['max_value'] and apt.get('price', float('inf')) > f['max_value']:
                    return False
            elif f['filter_type'] == 'rooms':
                if f['min_value'] and apt.get('rooms', 0) < f['min_value']:
                    return False
                if f['max_value'] and apt.get('rooms', float('inf')) > f['max_value']:
                    return False
            elif f['filter_type'] == 'neighborhood':
                if f['text_value'] and f['text_value'].lower() not in apt.get('neighborhood', '').lower():
                    return False

        return True

    # ============ Settings ============

    def get_setting(self, key: str, default=None) -> str:
        """Get a setting value"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT value FROM settings WHERE key = ?', (key,))
            row = cursor.fetchone()
            return row['value'] if row else default

    def set_setting(self, key: str, value: str):
        """Set a setting value"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            ''', (key, value, datetime.now().isoformat()))

    # ============ Logging ============

    def log_scrape_event(self, event_type: str, details: Dict = None):
        """Log a scrape event"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'INSERT INTO scrape_logs (event_type, details) VALUES (?, ?)',
                (event_type, json.dumps(details) if details else None)
            )

    def get_scrape_stats(self, hours: int = 24) -> Dict:
        """Get scraping statistics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()

            cursor.execute('''
                SELECT event_type, COUNT(*) as count
                FROM scrape_logs
                WHERE created_at > ?
                GROUP BY event_type
            ''', (cutoff,))

            stats = {row['event_type']: row['count'] for row in cursor.fetchall()}
            return stats

    # ============ Daily Summary ============

    def update_daily_summary(self, new_apts: int = 0, price_drops: int = 0,
                            price_increases: int = 0, removed: int = 0):
        """Update today's summary"""
        today = datetime.now().strftime('%Y-%m-%d')
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO daily_summaries (date, new_apartments, price_drops, price_increases, removed)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    new_apartments = daily_summaries.new_apartments + excluded.new_apartments,
                    price_drops = daily_summaries.price_drops + excluded.price_drops,
                    price_increases = daily_summaries.price_increases + excluded.price_increases,
                    removed = daily_summaries.removed + excluded.removed
            ''', (today, new_apts, price_drops, price_increases, removed))

    def get_daily_summary(self, date: str = None) -> Optional[Dict]:
        """Get summary for a specific date"""
        if not date:
            date = datetime.now().strftime('%Y-%m-%d')
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM daily_summaries WHERE date = ?', (date,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def mark_summary_sent(self, date: str = None):
        """Mark daily summary as sent"""
        if not date:
            date = datetime.now().strftime('%Y-%m-%d')
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE daily_summaries SET summary_sent = 1 WHERE date = ?',
                (date,)
            )

    # ============ Export ============

    def export_to_csv(self, filepath: str):
        """Export apartments to CSV"""
        import csv
        apartments = self.get_all_apartments(active_only=False)

        if not apartments:
            return False

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=apartments[0].keys())
            writer.writeheader()
            writer.writerows(apartments)

        return True

    def export_price_history_csv(self, filepath: str, apt_id: str = None):
        """Export price history to CSV"""
        import csv
        with self.get_connection() as conn:
            cursor = conn.cursor()

            if apt_id:
                cursor.execute('''
                    SELECT a.title, ph.apartment_id, ph.price, ph.recorded_at
                    FROM price_history ph
                    JOIN apartments a ON ph.apartment_id = a.id
                    WHERE ph.apartment_id = ?
                    ORDER BY ph.recorded_at
                ''', (apt_id,))
            else:
                cursor.execute('''
                    SELECT a.title, ph.apartment_id, ph.price, ph.recorded_at
                    FROM price_history ph
                    JOIN apartments a ON ph.apartment_id = a.id
                    ORDER BY ph.apartment_id, ph.recorded_at
                ''')

            rows = cursor.fetchall()

            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['title', 'apartment_id', 'price', 'recorded_at'])
                for row in rows:
                    writer.writerow(row)

        return True

    # ============ Backup ============

    def backup(self, backup_path: str = None):
        """Create database backup"""
        if not backup_path:
            backup_path = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"

        with self.get_connection() as conn:
            backup_conn = sqlite3.connect(backup_path)
            conn.backup(backup_conn)
            backup_conn.close()

        logger.info(f"Database backed up to {backup_path}")
        return backup_path
