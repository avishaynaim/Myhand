"""
Database wrapper that supports both SQLite and PostgreSQL
Automatically detects which database to use based on environment
"""
import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def get_database():
    """
    Factory function that returns the appropriate database instance
    based on environment configuration.

    Priority:
    1. PostgreSQL (if DATABASE_URL is set)
    2. SQLite (fallback)
    """
    database_url = os.environ.get('DATABASE_URL')

    if database_url:
        logger.info("📊 Detected DATABASE_URL - using PostgreSQL")
        try:
            from database_postgres import PostgreSQLDatabase
            return PostgreSQLDatabase(database_url)
        except ImportError as e:
            logger.error(f"Failed to import PostgreSQL support: {e}")
            logger.warning("Falling back to SQLite...")
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL: {e}")
            logger.warning("Falling back to SQLite...")

    # Fallback to SQLite
    logger.info("📊 Using SQLite database")
    from database import Database

    # Determine SQLite path
    db_path = os.environ.get('DATABASE_PATH')
    if not db_path:
        data_dir = '/data'
        if os.path.exists(data_dir) and os.path.isdir(data_dir):
            db_path = os.path.join(data_dir, 'yad2_monitor.db')
            logger.info(f"✅ Using persistent storage: {db_path}")
        else:
            db_path = 'yad2_monitor.db'
            if 'RAILWAY_ENVIRONMENT' in os.environ:
                logger.error(f"❌ EPHEMERAL STORAGE: Data will be deleted on deploy!")
                logger.error(f"❌ Add PostgreSQL in Railway for persistence!")
            else:
                logger.info(f"📁 Using local storage: {db_path}")

    return Database(db_path)
