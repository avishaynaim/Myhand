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
        self._local = threading.local()  # Thread-local storage for connections
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
        """
        Get a thread-local database connection.
        Each thread gets its own connection for thread safety.
        """
        # Check if this thread already has a connection
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(
                self.db_path,
                timeout=30.0,  # Wait up to 30 seconds for lock
                # Note: check_same_thread=True (default) for safety
            )
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute('PRAGMA busy_timeout=30000')

        conn = self._local.conn
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        # Note: We don't close the connection here since it's reused per thread
        # Connections are closed when threads terminate or via explicit cleanup

    def close_connection(self):
        """Close the connection for the current thread."""
        if hasattr(self._local, 'conn') and self._local.conn is not None:
            try:
                self._local.conn.close()
                logger.debug("Closed database connection for current thread")
            except Exception as e:
                logger.warning(f"Error closing database connection: {e}")
            finally:
                self._local.conn = None

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

            # User-specific favorites (replaces old favorites table)
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
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
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

            # Create indexes
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

            logger.info(f"Database initialized at {self.db_path}")

            # Migrate data from old favorites table to new user_favorites table
            self._migrate_to_multi_user(cursor)

    def _migrate_to_multi_user(self, cursor):
        """Migrate existing single-user data to multi-user schema"""
        try:
            # Check if old favorites table exists and has data
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='favorites'")
            if cursor.fetchone():
                cursor.execute("SELECT COUNT(*) FROM favorites")
                old_count = cursor.fetchone()[0]

                if old_count > 0:
                    # Get the default chat ID from environment
                    import os
                    default_chat_id = os.environ.get('TELEGRAM_CHAT_ID')

                    if default_chat_id:
                        # Create default user if not exists
                        cursor.execute('''
                            INSERT OR IGNORE INTO telegram_users (chat_id, first_name, is_active)
                            VALUES (?, 'Default User', 1)
                        ''', (default_chat_id,))

                        # Migrate favorites
                        cursor.execute('''
                            INSERT OR IGNORE INTO user_favorites (chat_id, apartment_id, notes, added_at)
                            SELECT ?, apartment_id, notes, added_at
                            FROM favorites
                        ''', (default_chat_id,))

                        # Migrate ignored apartments
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ignored'")
                        if cursor.fetchone():
                            cursor.execute('''
                                INSERT OR IGNORE INTO user_ignored (chat_id, apartment_id, reason, ignored_at)
                                SELECT ?, apartment_id, reason, ignored_at
                                FROM ignored
                            ''', (default_chat_id,))

                        logger.info(f"Migrated {old_count} favorites to user {default_chat_id}")

                        # Optionally drop old tables after successful migration
                        # cursor.execute('DROP TABLE IF EXISTS favorites')
                        # cursor.execute('DROP TABLE IF EXISTS ignored')
                    else:
                        logger.warning("TELEGRAM_CHAT_ID not set - skipping favorites migration")
        except Exception as e:
            logger.error(f"Error during multi-user migration: {e}", exc_info=True)

    # ============ User Management Methods ============

    def add_or_update_user(self, chat_id: str, username: str = None, first_name: str = None, last_name: str = None, language_code: str = 'he'):
        """Add or update a Telegram user"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO telegram_users (chat_id, username, first_name, last_name, language_code, last_interaction)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(chat_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_name = excluded.last_name,
                    language_code = excluded.language_code,
                    last_interaction = CURRENT_TIMESTAMP
            ''', (chat_id, username, first_name, last_name, language_code))

            # Create default preferences if not exists
            cursor.execute('''
                INSERT OR IGNORE INTO user_preferences (chat_id)
                VALUES (?)
            ''', (chat_id,))

    def get_user(self, chat_id: str) -> Optional[Dict]:
        """Get user information"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM telegram_users WHERE chat_id = ?', (chat_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_all_active_users(self) -> List[Dict]:
        """Get all active users"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM telegram_users
                WHERE is_active = 1 AND is_paused = 0
                ORDER BY last_interaction DESC
            ''')
            return [dict(row) for row in cursor.fetchall()]

    def pause_user_notifications(self, chat_id: str, paused: bool = True):
        """Pause or resume notifications for a user"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE telegram_users
                SET is_paused = ?
                WHERE chat_id = ?
            ''', (1 if paused else 0, chat_id))

    def get_user_preferences(self, chat_id: str) -> Dict:
        """Get user preferences"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM user_preferences WHERE chat_id = ?', (chat_id,))
            row = cursor.fetchone()
            if row:
                return dict(row)
            else:
                # Return defaults
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
            fields = []
            values = []
            for key, value in kwargs.items():
                if key in ['instant_notifications', 'daily_digest', 'digest_hour', 'notification_types', 'preferences_json']:
                    fields.append(f"{key} = ?")
                    values.append(value)

            if fields:
                values.append(chat_id)
                cursor.execute(f'''
                    INSERT INTO user_preferences (chat_id, {', '.join(kwargs.keys())})
                    VALUES (?, {', '.join(['?'] * len(kwargs))})
                    ON CONFLICT(chat_id) DO UPDATE SET {', '.join(fields)}
                ''', values + [chat_id] + values)

    # ============ User Favorites Methods ============

    def add_user_favorite(self, chat_id: str, apt_id: str, notes: str = None):
        """Add apartment to user's favorites"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO user_favorites (chat_id, apartment_id, notes)
                VALUES (?, ?, ?)
            ''', (chat_id, apt_id, notes))

    def remove_user_favorite(self, chat_id: str, apt_id: str):
        """Remove from user's favorites"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM user_favorites
                WHERE chat_id = ? AND apartment_id = ?
            ''', (chat_id, apt_id))

    def get_user_favorites(self, chat_id: str) -> List[Dict]:
        """Get user's favorites with apartment details"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT a.*, f.notes, f.added_at as favorited_at
                FROM apartments a
                JOIN user_favorites f ON a.id = f.apartment_id
                WHERE f.chat_id = ?
                ORDER BY f.added_at DESC
            ''', (chat_id,))
            return [dict(row) for row in cursor.fetchall()]

    def is_user_favorite(self, chat_id: str, apt_id: str) -> bool:
        """Check if apartment is in user's favorites"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 1 FROM user_favorites
                WHERE chat_id = ? AND apartment_id = ?
            ''', (chat_id, apt_id))
            return cursor.fetchone() is not None

    def add_user_ignored(self, chat_id: str, apt_id: str, reason: str = None):
        """Add apartment to user's ignored list"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO user_ignored (chat_id, apartment_id, reason)
                VALUES (?, ?, ?)
            ''', (chat_id, apt_id, reason))

    def is_user_ignored(self, chat_id: str, apt_id: str) -> bool:
        """Check if apartment is in user's ignored list"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 1 FROM user_ignored
                WHERE chat_id = ? AND apartment_id = ?
            ''', (chat_id, apt_id))
            return cursor.fetchone() is not None

    def get_user_filters(self, chat_id: str, active_only: bool = True) -> List[Dict]:
        """Get user's filters"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if active_only:
                cursor.execute('''
                    SELECT * FROM user_filters
                    WHERE chat_id = ? AND is_active = 1
                    ORDER BY created_at DESC
                ''', (chat_id,))
            else:
                cursor.execute('''
                    SELECT * FROM user_filters
                    WHERE chat_id = ?
                    ORDER BY created_at DESC
                ''', (chat_id,))
            return [dict(row) for row in cursor.fetchall()]

    def add_user_filter(self, chat_id: str, name: str, filter_type: str, min_value=None, max_value=None, text_value=None):
        """Add a filter for user"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO user_filters (chat_id, name, filter_type, min_value, max_value, text_value)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (chat_id, name, filter_type, min_value, max_value, text_value))

    def remove_user_filter(self, chat_id: str, filter_id: int):
        """Remove user's filter"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM user_filters
                WHERE chat_id = ? AND id = ?
            ''', (chat_id, filter_id))

    def toggle_user_filter(self, chat_id: str, filter_id: int, is_active: bool):
        """Toggle user's filter active state"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE user_filters
                SET is_active = ?
                WHERE chat_id = ? AND id = ?
            ''', (1 if is_active else 0, chat_id, filter_id))

    def apartment_matches_user_filters(self, chat_id: str, apartment: Dict) -> bool:
        """Check if apartment matches user's active filters"""
        filters = self.get_user_filters(chat_id, active_only=True)
        if not filters:
            return True  # No filters means all apartments match

        for f in filters:
            filter_type = f['filter_type']
            if filter_type == 'price':
                if f['min_value'] and apartment.get('price', 0) < f['min_value']:
                    return False
                if f['max_value'] and apartment.get('price', 0) > f['max_value']:
                    return False
            elif filter_type == 'rooms':
                if f['min_value'] and apartment.get('rooms', 0) < f['min_value']:
                    return False
                if f['max_value'] and apartment.get('rooms', 0) > f['max_value']:
                    return False
            elif filter_type == 'sqm':
                if f['min_value'] and apartment.get('sqm', 0) < f['min_value']:
                    return False
                if f['max_value'] and apartment.get('sqm', 0) > f['max_value']:
                    return False
            elif filter_type == 'city' and f['text_value']:
                if apartment.get('city', '').lower() != f['text_value'].lower():
                    return False
            elif filter_type == 'neighborhood' and f['text_value']:
                if apartment.get('neighborhood', '').lower() != f['text_value'].lower():
                    return False

        return True

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

            # Record price if changed or new
            if is_new or price_changed:
                if apt.get('price'):
                    self.add_price_history(apt['id'], apt['price'])

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
