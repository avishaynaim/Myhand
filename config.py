"""
Configuration Module for Yad2 Monitor
Validates environment variables and provides centralized configuration.
"""

import os
import re
import sys
import logging
from typing import Optional, List

logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """Configuration validation error."""
    pass


class Config:
    """Application configuration with validation."""

    # Required environment variables
    TELEGRAM_BOT_TOKEN: str
    TELEGRAM_CHAT_ID: str

    # Optional with defaults
    DATABASE_PATH: str = "yad2_monitor.db"
    LOG_LEVEL: str = "INFO"
    PORT: int = 5000
    HOST: str = "0.0.0.0"

    # API settings
    API_KEY: Optional[str] = None
    RATE_LIMIT_PER_HOUR: int = 100
    RATE_LIMIT_PER_MINUTE: int = 20

    # CORS
    ALLOWED_ORIGINS: List[str] = ["*"]

    # Dashboard
    DASHBOARD_URL: str = "http://localhost:5000"
    ENABLE_WEB: bool = True
    WEB_PORT: int = 5000

    # Notifications
    INSTANT_NOTIFICATIONS: bool = True
    DAILY_DIGEST_ENABLED: bool = True
    DAILY_DIGEST_HOUR: int = 20

    # Scraping
    MIN_INTERVAL_MINUTES: int = 60
    MAX_INTERVAL_MINUTES: int = 90
    HTTP_TIMEOUT_SECONDS: int = 15
    MAX_RETRIES: int = 3

    # Server identification
    SERVER_NAME: Optional[str] = None

    @classmethod
    def load_from_env(cls):
        """
        Load configuration from environment variables.

        Returns:
            Config instance with validated settings

        Raises:
            ConfigError: If required variables are missing or invalid
        """
        config = cls()

        try:
            # Required variables
            config.TELEGRAM_BOT_TOKEN = cls._get_required_env('TELEGRAM_BOT_TOKEN')
            config.TELEGRAM_CHAT_ID = cls._get_required_env('TELEGRAM_CHAT_ID')

            # Validate token format
            if not re.match(r'^\d+:[A-Za-z0-9_-]+$', config.TELEGRAM_BOT_TOKEN):
                raise ConfigError(
                    "Invalid TELEGRAM_BOT_TOKEN format. "
                    "Expected format: '123456789:ABCdefGHIjklMNOpqrSTUvwxYZ'"
                )

            # Validate chat ID format (should be numeric or start with -)
            if not re.match(r'^-?\d+$', config.TELEGRAM_CHAT_ID):
                raise ConfigError(
                    "Invalid TELEGRAM_CHAT_ID format. "
                    "Expected numeric value (e.g., '123456789' or '-100123456789' for groups)"
                )

            # Optional string variables
            config.DATABASE_PATH = os.getenv('DATABASE_PATH', cls.DATABASE_PATH)
            config.LOG_LEVEL = os.getenv('LOG_LEVEL', cls.LOG_LEVEL).upper()
            config.HOST = os.getenv('HOST', cls.HOST)
            config.API_KEY = os.getenv('API_KEY')
            config.DASHBOARD_URL = os.getenv('DASHBOARD_URL', cls.DASHBOARD_URL)
            config.SERVER_NAME = os.getenv('SERVER_NAME') or \
                                 os.getenv('RAILWAY_SERVICE_NAME') or \
                                 os.getenv('RAILWAY_PROJECT_NAME')

            # Validate log level
            valid_log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
            if config.LOG_LEVEL not in valid_log_levels:
                logger.warning(f"Invalid LOG_LEVEL '{config.LOG_LEVEL}', using INFO")
                config.LOG_LEVEL = 'INFO'

            # Optional integer variables
            config.PORT = cls._get_int_env('PORT', cls.PORT)
            config.WEB_PORT = cls._get_int_env('WEB_PORT', cls.WEB_PORT)
            config.RATE_LIMIT_PER_HOUR = cls._get_int_env('RATE_LIMIT_PER_HOUR', cls.RATE_LIMIT_PER_HOUR)
            config.RATE_LIMIT_PER_MINUTE = cls._get_int_env('RATE_LIMIT_PER_MINUTE', cls.RATE_LIMIT_PER_MINUTE)
            config.DAILY_DIGEST_HOUR = cls._get_int_env('DAILY_DIGEST_HOUR', cls.DAILY_DIGEST_HOUR)
            config.MIN_INTERVAL_MINUTES = cls._get_int_env('MIN_INTERVAL_MINUTES', cls.MIN_INTERVAL_MINUTES)
            config.MAX_INTERVAL_MINUTES = cls._get_int_env('MAX_INTERVAL_MINUTES', cls.MAX_INTERVAL_MINUTES)
            config.HTTP_TIMEOUT_SECONDS = cls._get_int_env('HTTP_TIMEOUT_SECONDS', cls.HTTP_TIMEOUT_SECONDS)
            config.MAX_RETRIES = cls._get_int_env('MAX_RETRIES', cls.MAX_RETRIES)

            # Validate integer ranges
            if not 1024 <= config.PORT <= 65535:
                raise ConfigError(f"PORT must be between 1024 and 65535, got {config.PORT}")

            if not 1024 <= config.WEB_PORT <= 65535:
                raise ConfigError(f"WEB_PORT must be between 1024 and 65535, got {config.WEB_PORT}")

            if not 0 <= config.DAILY_DIGEST_HOUR <= 23:
                raise ConfigError(f"DAILY_DIGEST_HOUR must be between 0 and 23, got {config.DAILY_DIGEST_HOUR}")

            if config.MIN_INTERVAL_MINUTES >= config.MAX_INTERVAL_MINUTES:
                raise ConfigError(
                    f"MIN_INTERVAL_MINUTES ({config.MIN_INTERVAL_MINUTES}) must be less than "
                    f"MAX_INTERVAL_MINUTES ({config.MAX_INTERVAL_MINUTES})"
                )

            # Optional boolean variables
            config.ENABLE_WEB = cls._get_bool_env('ENABLE_WEB', cls.ENABLE_WEB)
            config.INSTANT_NOTIFICATIONS = cls._get_bool_env('INSTANT_NOTIFICATIONS', cls.INSTANT_NOTIFICATIONS)
            config.DAILY_DIGEST_ENABLED = cls._get_bool_env('DAILY_DIGEST_ENABLED', cls.DAILY_DIGEST_ENABLED)

            # CORS origins
            origins_str = os.getenv('ALLOWED_ORIGINS', '*')
            if origins_str:
                config.ALLOWED_ORIGINS = [origin.strip() for origin in origins_str.split(',')]
            else:
                config.ALLOWED_ORIGINS = ['*']

            # Use PORT if WEB_PORT not explicitly set
            if not os.getenv('WEB_PORT') and os.getenv('PORT'):
                config.WEB_PORT = config.PORT

            return config

        except ConfigError:
            raise
        except Exception as e:
            raise ConfigError(f"Error loading configuration: {e}")

    @staticmethod
    def _get_required_env(key: str) -> str:
        """
        Get required environment variable or raise error.

        Args:
            key: Environment variable name

        Returns:
            Environment variable value

        Raises:
            ConfigError: If variable is missing
        """
        value = os.getenv(key)
        if not value:
            raise ConfigError(
                f"Missing required environment variable: {key}\n"
                f"Please set {key} in your environment or .env file"
            )
        return value

    @staticmethod
    def _get_int_env(key: str, default: int) -> int:
        """
        Get integer environment variable with default.

        Args:
            key: Environment variable name
            default: Default value if not set

        Returns:
            Integer value

        Raises:
            ConfigError: If value is not a valid integer
        """
        value_str = os.getenv(key)
        if not value_str:
            return default

        try:
            return int(value_str)
        except ValueError:
            raise ConfigError(f"{key} must be an integer, got '{value_str}'")

    @staticmethod
    def _get_bool_env(key: str, default: bool) -> bool:
        """
        Get boolean environment variable with default.

        Args:
            key: Environment variable name
            default: Default value if not set

        Returns:
            Boolean value
        """
        value_str = os.getenv(key)
        if not value_str:
            return default

        return value_str.lower() in ('true', '1', 'yes', 'on')

    def validate(self):
        """
        Validate the configuration after loading.

        Raises:
            ConfigError: If configuration is invalid
        """
        # Additional validation logic can be added here
        if self.API_KEY and len(self.API_KEY) < 16:
            logger.warning(
                "API_KEY is shorter than 16 characters. "
                "Consider using a stronger key (e.g., 'openssl rand -hex 32')"
            )

        # Warn if running on 0.0.0.0
        if self.HOST == '0.0.0.0':
            logger.warning(
                "Server configured to listen on 0.0.0.0 (all interfaces). "
                "Ensure firewall is properly configured."
            )

        # Warn if CORS allows all origins in production
        if '*' in self.ALLOWED_ORIGINS and self.API_KEY:
            logger.warning(
                "CORS is configured to allow all origins (*). "
                "Consider restricting to specific domains in production."
            )

    def __repr__(self):
        """String representation (hides sensitive values)."""
        return (
            f"Config("
            f"database={self.DATABASE_PATH}, "
            f"port={self.PORT}, "
            f"host={self.HOST}, "
            f"api_key={'***' if self.API_KEY else 'None'}, "
            f"telegram_configured={bool(self.TELEGRAM_BOT_TOKEN and self.TELEGRAM_CHAT_ID)}"
            f")"
        )

    def get_summary(self) -> str:
        """Get human-readable configuration summary."""
        lines = [
            "=== Configuration Summary ===",
            f"Database: {self.DATABASE_PATH}",
            f"Log Level: {self.LOG_LEVEL}",
            f"Web Server: {self.HOST}:{self.WEB_PORT}",
            f"Dashboard URL: {self.DASHBOARD_URL}",
            f"API Key: {'✓ Configured' if self.API_KEY else '✗ Not configured (API endpoints unprotected)'}",
            f"Rate Limits: {self.RATE_LIMIT_PER_HOUR}/hour, {self.RATE_LIMIT_PER_MINUTE}/minute",
            f"CORS Origins: {', '.join(self.ALLOWED_ORIGINS)}",
            f"Telegram Bot: ✓ Configured" if self.TELEGRAM_BOT_TOKEN else "✗ Not configured",
            f"Instant Notifications: {'Enabled' if self.INSTANT_NOTIFICATIONS else 'Disabled'}",
            f"Daily Digest: {'Enabled' if self.DAILY_DIGEST_ENABLED else 'Disabled'} (at {self.DAILY_DIGEST_HOUR}:00)",
            f"Scraping Interval: {self.MIN_INTERVAL_MINUTES}-{self.MAX_INTERVAL_MINUTES} minutes",
            f"Server Name: {self.SERVER_NAME or 'Not set'}",
            "============================",
        ]
        return '\n'.join(lines)


def validate_environment():
    """
    Validate environment and return configuration.
    Exits with error if validation fails.

    Returns:
        Config instance
    """
    try:
        config = Config.load_from_env()
        config.validate()
        logger.info("Configuration loaded successfully")
        logger.debug(config.get_summary())
        return config
    except ConfigError as e:
        logger.error(f"❌ Configuration Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error loading configuration: {e}", exc_info=True)
        sys.exit(1)
