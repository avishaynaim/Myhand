"""
PostgreSQL Database Module for Yad2 Monitor
Compatible with the SQLite Database interface
"""
import psycopg2
import psycopg2.extras
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


class PostgreSQLDatabase:
    """PostgreSQL implementation compatible with SQLite Database interface"""

    def __init__(self, database_url: str):
        self.database_url = database_url
        logger.info(f"🐘 Initializing PostgreSQL database")
        self.init_database()

    @contextmanager
    def get_connection(self):
        """Get a PostgreSQL connection"""
        conn = psycopg2.connect(self.database_url)
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def init_database(self):
        """Initialize all PostgreSQL tables (converting SQLite schema)"""
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
                    id SERIAL PRIMARY KEY,
                    apartment_id TEXT NOT NULL,
                    price INTEGER NOT NULL,
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (apartment_id) REFERENCES apartments(id)
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

            # Search URLs table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS search_urls (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_scraped TIMESTAMP
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

            # Scrape logs table (same as scrape_events in other version)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scrape_logs (
                    id SERIAL PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Old favorites table (for backwards compatibility)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS favorites (
                    apartment_id TEXT PRIMARY KEY,
                    notes TEXT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (apartment_id) REFERENCES apartments(id)
                )
            ''')

            # Old ignored table (for backwards compatibility)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ignored (
                    apartment_id TEXT PRIMARY KEY,
                    reason TEXT,
                    ignored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (apartment_id) REFERENCES apartments(id)
                )
            ''')

            # Old filters table (for backwards compatibility)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS filters (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    filter_type TEXT NOT NULL,
                    min_value REAL,
                    max_value REAL,
                    text_value TEXT,
                    is_active INTEGER DEFAULT 1
                )
            ''')

            # Notification queue table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS notification_queue (
                    id SERIAL PRIMARY KEY,
                    notification_type TEXT NOT NULL,
                    apartment_id TEXT,
                    message TEXT NOT NULL,
                    priority INTEGER DEFAULT 0,
                    scheduled_for TIMESTAMP,
                    sent_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Telegram users table for multi-user support
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS telegram_users (
                    chat_id TEXT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    language_code TEXT DEFAULT 'he',
                    is_active INTEGER DEFAULT 1,
                    is_paused INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_interaction TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # User preferences table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_preferences (
                    chat_id TEXT PRIMARY KEY,
                    instant_notifications INTEGER DEFAULT 1,
                    daily_digest INTEGER DEFAULT 1,
                    digest_hour INTEGER DEFAULT 20,
                    notification_types TEXT DEFAULT 'new,price_drop',
                    preferences_json TEXT,
                    FOREIGN KEY (chat_id) REFERENCES telegram_users(chat_id)
                )
            ''')

            # User-specific favorites
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_favorites (
                    chat_id TEXT NOT NULL,
                    apartment_id TEXT NOT NULL,
                    notes TEXT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (chat_id, apartment_id),
                    FOREIGN KEY (chat_id) REFERENCES telegram_users(chat_id),
                    FOREIGN KEY (apartment_id) REFERENCES apartments(id)
                )
            ''')

            # User-specific ignored apartments
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_ignored (
                    chat_id TEXT NOT NULL,
                    apartment_id TEXT NOT NULL,
                    reason TEXT,
                    ignored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (chat_id, apartment_id),
                    FOREIGN KEY (chat_id) REFERENCES telegram_users(chat_id),
                    FOREIGN KEY (apartment_id) REFERENCES apartments(id)
                )
            ''')

            # User-specific filters
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_filters (
                    id SERIAL PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    filter_type TEXT NOT NULL,
                    min_value REAL,
                    max_value REAL,
                    text_value TEXT,
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (chat_id) REFERENCES telegram_users(chat_id)
                )
            ''')

            # Create indexes for performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_apartments_price ON apartments(price)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_apartments_location ON apartments(location)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_apartments_last_seen ON apartments(last_seen)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_history_apt ON price_history(apartment_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_history_date ON price_history(recorded_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_scrape_logs_type ON scrape_logs(event_type)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_favorites_chat ON user_favorites(chat_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_favorites_apt ON user_favorites(apartment_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_ignored_chat ON user_ignored(chat_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_filters_chat ON user_filters(chat_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_telegram_users_active ON telegram_users(is_active)')

            logger.info("✅ PostgreSQL tables initialized successfully")

    # All other methods from database.py need to be copied here
    # For now, let's implement the most critical ones

    def upsert_apartment(self, apartment: Dict) -> Tuple[bool, bool]:
        """Insert or update apartment - returns (updated, is_new)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Check if exists
            cursor.execute('SELECT id, price FROM apartments WHERE id = %s', (apartment['id'],))
            existing = cursor.fetchone()

            if existing:
                # Update existing
                cursor.execute('''
                    UPDATE apartments SET
                        title = %s, price = %s, price_text = %s, location = %s,
                        street_address = %s, item_info = %s, link = %s, image_url = %s,
                        rooms = %s, sqm = %s, floor = %s, neighborhood = %s, city = %s,
                        data_updated_at = %s, last_seen = CURRENT_TIMESTAMP, is_active = 1,
                        raw_data = %s
                    WHERE id = %s
                ''', (
                    apartment.get('title'), apartment.get('price'), apartment.get('price_text'),
                    apartment.get('location'), apartment.get('street_address'), apartment.get('item_info'),
                    apartment.get('link'), apartment.get('image_url'), apartment.get('rooms'),
                    apartment.get('sqm'), apartment.get('floor'), apartment.get('neighborhood'),
                    apartment.get('city'), apartment.get('data_updated_at'),
                    json.dumps(apartment, ensure_ascii=False), apartment['id']
                ))
                return (True, False)
            else:
                # Insert new
                cursor.execute('''
                    INSERT INTO apartments (id, title, price, price_text, location, street_address,
                        item_info, link, image_url, rooms, sqm, floor, neighborhood, city,
                        data_updated_at, last_seen, is_active, raw_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 1, %s)
                ''', (
                    apartment['id'], apartment.get('title'), apartment.get('price'),
                    apartment.get('price_text'), apartment.get('location'), apartment.get('street_address'),
                    apartment.get('item_info'), apartment.get('link'), apartment.get('image_url'),
                    apartment.get('rooms'), apartment.get('sqm'), apartment.get('floor'),
                    apartment.get('neighborhood'), apartment.get('city'), apartment.get('data_updated_at'),
                    json.dumps(apartment, ensure_ascii=False)
                ))
                return (True, True)

    def get_apartment(self, apartment_id: str) -> Optional[Dict]:
        """Get apartment by ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute('SELECT * FROM apartments WHERE id = %s', (apartment_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_all_apartments(self, active_only: bool = True) -> List[Dict]:
        """Get all apartments"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if active_only:
                cursor.execute('SELECT * FROM apartments WHERE is_active = 1 ORDER BY first_seen DESC')
            else:
                cursor.execute('SELECT * FROM apartments ORDER BY first_seen DESC')
            return [dict(row) for row in cursor.fetchall()]

    def get_setting(self, key: str, default=None) -> str:
        """Get a setting value"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT value FROM settings WHERE key = %s', (key,))
            row = cursor.fetchone()
            return row[0] if row else default

    def set_setting(self, key: str, value: str):
        """Set a setting value"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO settings (key, value, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
            ''', (key, value))

    def get_favorites(self) -> List[Dict]:
        """Get all favorites"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute('''
                SELECT a.* FROM apartments a
                INNER JOIN favorites f ON a.id = f.apartment_id
                ORDER BY f.added_at DESC
            ''')
            return [dict(row) for row in cursor.fetchall()]

    def get_search_urls(self, active_only: bool = True) -> List[Dict]:
        """Get all search URLs"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if active_only:
                cursor.execute('SELECT * FROM search_urls WHERE active = 1')
            else:
                cursor.execute('SELECT * FROM search_urls')
            return [dict(row) for row in cursor.fetchall()]

    def get_scrape_stats(self, hours: int = 24) -> Dict:
        """Get scraping statistics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cutoff = datetime.now() - timedelta(hours=hours)
            cursor.execute('''
                SELECT event_type, COUNT(*) as count
                FROM scrape_logs
                WHERE created_at >= %s
                GROUP BY event_type
            ''', (cutoff,))
            return {row[0]: row[1] for row in cursor.fetchall()}

    def log_scrape_event(self, event_type: str, details: Dict = None):
        """Log a scrape event"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO scrape_logs (event_type, details)
                VALUES (%s, %s)
            ''', (event_type, json.dumps(details) if details else None))

    def get_daily_summary(self, date: str = None) -> Optional[Dict]:
        """Get summary for a specific date"""
        if not date:
            date = datetime.now().strftime('%Y-%m-%d')
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute('SELECT * FROM daily_summaries WHERE date = %s', (date,))
            row = cursor.fetchone()
            return dict(row) if row else None

    # Additional multi-user methods stubs
    def get_all_active_users(self) -> List[Dict]:
        """Get all active Telegram users"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute('SELECT * FROM telegram_users WHERE is_active = 1 AND notifications_enabled = 1')
            return [dict(row) for row in cursor.fetchall()]

    def add_or_update_user(self, chat_id: str, username: str = None, first_name: str = None, last_name: str = None):
        """Add or update a Telegram user"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO telegram_users (chat_id, username, first_name, last_name, last_seen)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (chat_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    last_seen = CURRENT_TIMESTAMP
            ''', (chat_id, username, first_name, last_name))

    def apartment_matches_user_filters(self, chat_id: str, apartment: Dict) -> bool:
        """Check if apartment matches user's filters - simplified version"""
        # For now, return True (no filtering)
        # TODO: Implement filter matching logic
        return True

    def is_user_ignored(self, chat_id: str, apartment_id: str) -> bool:
        """Check if user has ignored an apartment"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM user_ignored WHERE chat_id = %s AND apartment_id = %s',
                         (chat_id, apartment_id))
            return cursor.fetchone() is not None

    # ============ Price History Methods ============

    def add_price_history(self, apartment_id: str, price: int):
        """Add price history entry"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('INSERT INTO price_history (apartment_id, price) VALUES (%s, %s)',
                         (apartment_id, price))

    def get_price_history(self, apartment_id: str, limit: int = 50) -> List[Dict]:
        """Get price history for apartment"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute('''
                SELECT price, recorded_at FROM price_history
                WHERE apartment_id = %s
                ORDER BY recorded_at DESC
                LIMIT %s
            ''', (apartment_id, limit))
            return [dict(row) for row in cursor.fetchall()]

    def get_price_changes(self, days: int = 7) -> List[Dict]:
        """Get recent price changes"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cutoff = datetime.now() - timedelta(days=days)
            cursor.execute('''
                SELECT a.id, a.title, a.link,
                       ph1.price as old_price, ph2.price as new_price,
                       ph2.recorded_at
                FROM apartments a
                JOIN price_history ph1 ON a.id = ph1.apartment_id
                JOIN price_history ph2 ON a.id = ph2.apartment_id
                WHERE ph2.recorded_at > %s
                AND ph1.id = (
                    SELECT id FROM price_history
                    WHERE apartment_id = a.id AND recorded_at < ph2.recorded_at
                    ORDER BY recorded_at DESC LIMIT 1
                )
                AND ph1.price != ph2.price
                ORDER BY ph2.recorded_at DESC
            ''', (cutoff,))
            return [dict(row) for row in cursor.fetchall()]

    # ============ User Favorites Methods ============

    def add_user_favorite(self, chat_id: str, apartment_id: str, notes: str = None):
        """Add apartment to user's favorites"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO user_favorites (chat_id, apartment_id, notes)
                VALUES (%s, %s, %s)
                ON CONFLICT (chat_id, apartment_id) DO UPDATE SET notes = EXCLUDED.notes
            ''', (chat_id, apartment_id, notes))

    def remove_user_favorite(self, chat_id: str, apartment_id: str):
        """Remove from user's favorites"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM user_favorites WHERE chat_id = %s AND apartment_id = %s',
                         (chat_id, apartment_id))

    def get_user_favorites(self, chat_id: str) -> List[Dict]:
        """Get user's favorites with apartment details"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute('''
                SELECT a.*, f.notes, f.added_at as favorited_at
                FROM apartments a
                JOIN user_favorites f ON a.id = f.apartment_id
                WHERE f.chat_id = %s
                ORDER BY f.added_at DESC
            ''', (chat_id,))
            return [dict(row) for row in cursor.fetchall()]

    def is_user_favorite(self, chat_id: str, apartment_id: str) -> bool:
        """Check if apartment is in user's favorites"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM user_favorites WHERE chat_id = %s AND apartment_id = %s',
                         (chat_id, apartment_id))
            return cursor.fetchone() is not None

    def add_user_ignored(self, chat_id: str, apartment_id: str, reason: str = None):
        """Add apartment to user's ignored list"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO user_ignored (chat_id, apartment_id, reason)
                VALUES (%s, %s, %s)
                ON CONFLICT (chat_id, apartment_id) DO UPDATE SET reason = EXCLUDED.reason
            ''', (chat_id, apartment_id, reason))

    # ============ User Filter Methods ============

    def get_user_filters(self, chat_id: str, active_only: bool = True) -> List[Dict]:
        """Get user's filters"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if active_only:
                cursor.execute('SELECT * FROM user_filters WHERE chat_id = %s AND is_active = 1 ORDER BY created_at DESC',
                             (chat_id,))
            else:
                cursor.execute('SELECT * FROM user_filters WHERE chat_id = %s ORDER BY created_at DESC',
                             (chat_id,))
            return [dict(row) for row in cursor.fetchall()]

    def add_user_filter(self, chat_id: str, name: str, filter_type: str, min_value=None, max_value=None, text_value=None):
        """Add a filter for user"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO user_filters (chat_id, name, filter_type, min_value, max_value, text_value)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (chat_id, name, filter_type, min_value, max_value, text_value))

    def remove_user_filter(self, chat_id: str, filter_id: int):
        """Remove user's filter"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM user_filters WHERE chat_id = %s AND id = %s',
                         (chat_id, filter_id))

    def toggle_user_filter(self, chat_id: str, filter_id: int, is_active: bool):
        """Toggle user's filter active state"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('UPDATE user_filters SET is_active = %s WHERE chat_id = %s AND id = %s',
                         (1 if is_active else 0, chat_id, filter_id))

    # ============ User Preferences Methods ============

    def get_user_preferences(self, chat_id: str) -> Dict:
        """Get user preferences"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute('SELECT * FROM user_preferences WHERE chat_id = %s', (chat_id,))
            row = cursor.fetchone()
            if row:
                return dict(row)
            else:
                return {
                    'chat_id': chat_id,
                    'instant_notifications': 1,
                    'daily_digest': 1,
                    'digest_hour': 20,
                    'notification_types': 'new,price_drop'
                }

    def update_user_preferences(self, chat_id: str, **kwargs):
        """Update user preferences"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            valid_fields = ['instant_notifications', 'daily_digest', 'digest_hour', 'notification_types', 'preferences_json']
            fields = []
            values = []

            for key, value in kwargs.items():
                if key in valid_fields:
                    fields.append(f"{key} = %s")
                    values.append(value)

            if fields:
                values.append(chat_id)
                cursor.execute(f'''
                    INSERT INTO user_preferences (chat_id) VALUES (%s)
                    ON CONFLICT (chat_id) DO UPDATE SET {', '.join(fields)}
                ''', [chat_id] + values)

    def pause_user_notifications(self, chat_id: str, paused: bool = True):
        """Pause or resume notifications for a user"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('UPDATE telegram_users SET is_paused = %s WHERE chat_id = %s',
                         (1 if paused else 0, chat_id))

    def get_user(self, chat_id: str) -> Optional[Dict]:
        """Get user information"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute('SELECT * FROM telegram_users WHERE chat_id = %s', (chat_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    # ============ Search URLs Methods ============

    def add_search_url(self, name: str, url: str) -> int:
        """Add a search URL to monitor"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('INSERT INTO search_urls (name, url) VALUES (%s, %s) RETURNING id',
                         (name, url))
            result = cursor.fetchone()
            return result[0] if result else None

    def update_search_url_scraped(self, url_id: int):
        """Update last scraped time"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('UPDATE search_urls SET last_scraped = CURRENT_TIMESTAMP WHERE id = %s',
                         (url_id,))

    # ============ Filtered Apartments ============

    def get_apartments_filtered(self, filters: Dict) -> List[Dict]:
        """Get apartments with filters applied"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            query = 'SELECT * FROM apartments WHERE is_active = 1'
            params = []

            if filters.get('min_price'):
                query += ' AND price >= %s'
                params.append(filters['min_price'])
            if filters.get('max_price'):
                query += ' AND price <= %s'
                params.append(filters['max_price'])
            if filters.get('min_rooms'):
                query += ' AND rooms >= %s'
                params.append(filters['min_rooms'])
            if filters.get('max_rooms'):
                query += ' AND rooms <= %s'
                params.append(filters['max_rooms'])
            if filters.get('min_sqm'):
                query += ' AND sqm >= %s'
                params.append(filters['min_sqm'])
            if filters.get('neighborhood'):
                query += ' AND neighborhood LIKE %s'
                params.append(f"%{filters['neighborhood']}%")
            if filters.get('city'):
                query += ' AND city LIKE %s'
                params.append(f"%{filters['city']}%")

            query += ' ORDER BY last_seen DESC'

            if filters.get('limit'):
                query += ' LIMIT %s'
                params.append(filters['limit'])

            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def mark_apartments_inactive(self, active_ids: set):
        """Mark apartments not in active_ids as inactive"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Get current active apartments
            cursor.execute('SELECT id FROM apartments WHERE is_active = 1')
            all_active = {row[0] for row in cursor.fetchall()}

            # Mark missing ones as inactive
            to_deactivate = all_active - active_ids
            if to_deactivate:
                cursor.execute(
                    'UPDATE apartments SET is_active = 0 WHERE id = ANY(%s)',
                    (list(to_deactivate),)
                )
                logger.info(f"Marked {len(to_deactivate)} apartments as inactive")

            return list(to_deactivate)

    # ============ Daily Summary Methods ============

    def update_daily_summary(self, new_apts: int = 0, price_drops: int = 0,
                            price_increases: int = 0, removed: int = 0):
        """Update today's summary"""
        today = datetime.now().strftime('%Y-%m-%d')
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO daily_summaries (date, new_apartments, price_drops, price_increases, removed)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (date) DO UPDATE SET
                    new_apartments = daily_summaries.new_apartments + EXCLUDED.new_apartments,
                    price_drops = daily_summaries.price_drops + EXCLUDED.price_drops,
                    price_increases = daily_summaries.price_increases + EXCLUDED.price_increases,
                    removed = daily_summaries.removed + EXCLUDED.removed
            ''', (today, new_apts, price_drops, price_increases, removed))

    def mark_summary_sent(self, date: str = None):
        """Mark daily summary as sent"""
        if not date:
            date = datetime.now().strftime('%Y-%m-%d')
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('UPDATE daily_summaries SET summary_sent = 1 WHERE date = %s', (date,))

    # ============ Old Global Favorites/Ignored (for backwards compatibility) ============

    def add_favorite(self, apartment_id: str, notes: str = None):
        """Add apartment to favorites (legacy single-user)"""
        # Use default user if TELEGRAM_CHAT_ID is set
        chat_id = os.environ.get('TELEGRAM_CHAT_ID')
        if chat_id:
            self.add_user_favorite(chat_id, apartment_id, notes)

    def remove_favorite(self, apartment_id: str):
        """Remove from favorites (legacy single-user)"""
        chat_id = os.environ.get('TELEGRAM_CHAT_ID')
        if chat_id:
            self.remove_user_favorite(chat_id, apartment_id)

    def is_favorite(self, apartment_id: str) -> bool:
        """Check if apartment is favorited (legacy single-user)"""
        chat_id = os.environ.get('TELEGRAM_CHAT_ID')
        if chat_id:
            return self.is_user_favorite(chat_id, apartment_id)
        return False

    def add_ignored(self, apartment_id: str, reason: str = None):
        """Add apartment to ignored list (legacy single-user)"""
        chat_id = os.environ.get('TELEGRAM_CHAT_ID')
        if chat_id:
            self.add_user_ignored(chat_id, apartment_id, reason)

    def remove_ignored(self, apartment_id: str):
        """Remove from ignored (legacy single-user)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM user_ignored WHERE apartment_id = %s', (apartment_id,))

    def get_ignored_ids(self) -> set:
        """Get set of ignored apartment IDs (legacy single-user)"""
        chat_id = os.environ.get('TELEGRAM_CHAT_ID')
        if chat_id:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT apartment_id FROM user_ignored WHERE chat_id = %s', (chat_id,))
                return {row[0] for row in cursor.fetchall()}
        return set()

    # ============ Old Global Filters (for backwards compatibility) ============

    def add_filter(self, name: str, filter_type: str, min_val=None, max_val=None, text_val=None):
        """Add a notification filter (legacy)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO filters (name, filter_type, min_value, max_value, text_value)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            ''', (name, filter_type, min_val, max_val, text_val))
            result = cursor.fetchone()
            return result[0] if result else None

    def get_active_filters(self) -> List[Dict]:
        """Get all active filters (legacy)"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute('SELECT * FROM filters WHERE is_active = 1')
            return [dict(row) for row in cursor.fetchall()]

    def apartment_passes_filters(self, apartment: Dict) -> bool:
        """Check if apartment passes all active filters (legacy)"""
        filters = self.get_active_filters()

        for f in filters:
            if f['filter_type'] == 'price':
                if f['min_value'] and apartment.get('price', 0) < f['min_value']:
                    return False
                if f['max_value'] and apartment.get('price', float('inf')) > f['max_value']:
                    return False
            elif f['filter_type'] == 'rooms':
                if f['min_value'] and apartment.get('rooms', 0) < f['min_value']:
                    return False
                if f['max_value'] and apartment.get('rooms', float('inf')) > f['max_value']:
                    return False
            elif f['filter_type'] == 'neighborhood':
                if f['text_value'] and f['text_value'].lower() not in apartment.get('neighborhood', '').lower():
                    return False

        return True

    # ============ Export Methods ============

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

    def export_price_history_csv(self, filepath: str, apartment_id: str = None):
        """Export price history to CSV"""
        import csv
        with self.get_connection() as conn:
            cursor = conn.cursor()

            if apartment_id:
                cursor.execute('''
                    SELECT a.title, ph.apartment_id, ph.price, ph.recorded_at
                    FROM price_history ph
                    JOIN apartments a ON ph.apartment_id = a.id
                    WHERE ph.apartment_id = %s
                    ORDER BY ph.recorded_at
                ''', (apartment_id,))
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

    # ============ Utility Methods ============

    def close_connection(self):
        """Close connection (no-op for PostgreSQL - connections are per-context)"""
        # PostgreSQL connections are closed automatically after each context manager exit
        pass

    def backup(self, backup_path: str = None):
        """Create database backup (PostgreSQL needs pg_dump - not implemented)"""
        logger.warning("⚠️  PostgreSQL backup requires pg_dump - use your hosting provider's backup tools")
        return None

    # Delegate remaining methods
    def __getattr__(self, name):
        """
        Fallback: if method not implemented in PostgreSQL, log a warning
        """
        logger.warning(f"⚠️  Method '{name}' not yet implemented for PostgreSQL")
        # Return a no-op function that returns None or empty list/dict based on method name
        if 'get_all' in name or 'get_list' in name:
            return lambda *args, **kwargs: []
        elif 'get' in name:
            return lambda *args, **kwargs: None
        else:
            return lambda *args, **kwargs: None
