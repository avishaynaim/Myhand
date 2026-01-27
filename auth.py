"""
Authentication module for API endpoints.
Provides API key authentication decorator.
"""

import os
import logging
from functools import wraps
from flask import request, jsonify

logger = logging.getLogger(__name__)


def require_api_key(f):
    """
    Decorator to require API key authentication for endpoints.

    API key can be provided via:
    - X-API-Key header
    - api_key query parameter

    Returns 401 Unauthorized if API key is missing or invalid.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check if API_KEY is configured
        expected_api_key = os.environ.get('API_KEY')

        # If no API key is configured, allow access (backward compatibility)
        if not expected_api_key:
            logger.warning("API_KEY not configured - API endpoints are unprotected!")
            return f(*args, **kwargs)

        # Get API key from request
        api_key = request.headers.get('X-API-Key') or request.args.get('api_key')

        if not api_key:
            logger.warning(f"API key missing for {request.path} from {request.remote_addr}")
            return jsonify({
                'error': 'אין הרשאה - נדרש מפתח API',
                'error_en': 'Unauthorized - API key required'
            }), 401

        if api_key != expected_api_key:
            logger.warning(f"Invalid API key for {request.path} from {request.remote_addr}")
            return jsonify({
                'error': 'מפתח API לא תקין',
                'error_en': 'Invalid API key'
            }), 401

        # API key is valid
        return f(*args, **kwargs)

    return decorated_function


def optional_api_key(f):
    """
    Decorator for endpoints that can work with or without authentication.
    If API key is provided, it must be valid.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        expected_api_key = os.environ.get('API_KEY')

        if not expected_api_key:
            return f(*args, **kwargs)

        api_key = request.headers.get('X-API-Key') or request.args.get('api_key')

        # If API key is provided, it must be valid
        if api_key and api_key != expected_api_key:
            logger.warning(f"Invalid API key for {request.path} from {request.remote_addr}")
            return jsonify({
                'error': 'מפתח API לא תקין',
                'error_en': 'Invalid API key'
            }), 401

        return f(*args, **kwargs)

    return decorated_function
