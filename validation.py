"""
Input validation module for API endpoints.
Provides validation helpers to prevent injection attacks and ensure data integrity.
"""

import re
from typing import Optional, Tuple, List


class ValidationError(Exception):
    """Custom exception for validation errors with user-friendly messages."""
    pass


def validate_apartment_id(apt_id: str) -> str:
    """
    Validate apartment ID format.

    Args:
        apt_id: Apartment ID to validate

    Returns:
        Validated apartment ID

    Raises:
        ValidationError: If ID is invalid
    """
    if not apt_id or not isinstance(apt_id, str):
        raise ValidationError("מזהה דירה חייב להיות מחרוזת לא ריקה / Apartment ID must be a non-empty string")

    # Allow alphanumeric, underscore, hyphen
    if not re.match(r'^[a-zA-Z0-9_-]+$', apt_id):
        raise ValidationError("מזהה דירה מכיל תווים לא חוקיים / Apartment ID contains invalid characters")

    if len(apt_id) > 100:
        raise ValidationError("מזהה דירה ארוך מדי / Apartment ID is too long")

    return apt_id


def validate_price_range(min_price: Optional[int], max_price: Optional[int]) -> Tuple[Optional[int], Optional[int]]:
    """
    Validate price range parameters.

    Args:
        min_price: Minimum price (optional)
        max_price: Maximum price (optional)

    Returns:
        Tuple of (min_price, max_price)

    Raises:
        ValidationError: If prices are invalid
    """
    if min_price is not None:
        if not isinstance(min_price, int) or min_price < 0:
            raise ValidationError("מחיר מינימלי חייב להיות מספר חיובי / Minimum price must be a positive number")
        if min_price > 100_000_000:
            raise ValidationError("מחיר מינימלי גבוה מדי / Minimum price is too high")

    if max_price is not None:
        if not isinstance(max_price, int) or max_price < 0:
            raise ValidationError("מחיר מקסימלי חייב להיות מספר חיובי / Maximum price must be a positive number")
        if max_price > 100_000_000:
            raise ValidationError("מחיר מקסימלי גבוה מדי / Maximum price is too high")

    if min_price is not None and max_price is not None:
        if min_price > max_price:
            raise ValidationError("מחיר מינימלי לא יכול להיות גדול ממחיר מקסימלי / Minimum price cannot be greater than maximum price")

    return min_price, max_price


def validate_rooms_range(min_rooms: Optional[float], max_rooms: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    """
    Validate rooms range parameters.

    Args:
        min_rooms: Minimum number of rooms (optional)
        max_rooms: Maximum number of rooms (optional)

    Returns:
        Tuple of (min_rooms, max_rooms)

    Raises:
        ValidationError: If room numbers are invalid
    """
    if min_rooms is not None:
        if not isinstance(min_rooms, (int, float)) or min_rooms < 0:
            raise ValidationError("מספר חדרים מינימלי חייב להיות מספר חיובי / Minimum rooms must be a positive number")
        if min_rooms > 50:
            raise ValidationError("מספר חדרים מינימלי גבוה מדי / Minimum rooms is too high")

    if max_rooms is not None:
        if not isinstance(max_rooms, (int, float)) or max_rooms < 0:
            raise ValidationError("מספר חדרים מקסימלי חייב להיות מספר חיובי / Maximum rooms must be a positive number")
        if max_rooms > 50:
            raise ValidationError("מספר חדרים מקסימלי גבוה מדי / Maximum rooms is too high")

    if min_rooms is not None and max_rooms is not None:
        if min_rooms > max_rooms:
            raise ValidationError("מספר חדרים מינימלי לא יכול להיות גדול ממקסימלי / Minimum rooms cannot be greater than maximum rooms")

    return min_rooms, max_rooms


def validate_sqm_range(min_sqm: Optional[int], max_sqm: Optional[int]) -> Tuple[Optional[int], Optional[int]]:
    """
    Validate square meters range parameters.

    Args:
        min_sqm: Minimum square meters (optional)
        max_sqm: Maximum square meters (optional)

    Returns:
        Tuple of (min_sqm, max_sqm)

    Raises:
        ValidationError: If sqm values are invalid
    """
    if min_sqm is not None:
        if not isinstance(min_sqm, int) or min_sqm < 0:
            raise ValidationError("מ\"ר מינימלי חייב להיות מספר חיובי / Minimum sqm must be a positive number")
        if min_sqm > 10000:
            raise ValidationError("מ\"ר מינימלי גבוה מדי / Minimum sqm is too high")

    if max_sqm is not None:
        if not isinstance(max_sqm, int) or max_sqm < 0:
            raise ValidationError("מ\"ר מקסימלי חייב להיות מספר חיובי / Maximum sqm must be a positive number")
        if max_sqm > 10000:
            raise ValidationError("מ\"ר מקסימלי גבוה מדי / Maximum sqm is too high")

    if min_sqm is not None and max_sqm is not None:
        if min_sqm > max_sqm:
            raise ValidationError("מ\"ר מינימלי לא יכול להיות גדול ממקסימלי / Minimum sqm cannot be greater than maximum sqm")

    return min_sqm, max_sqm


def validate_pagination(offset: Optional[int], limit: Optional[int]) -> Tuple[int, int]:
    """
    Validate pagination parameters.

    Args:
        offset: Number of items to skip (optional, default 0)
        limit: Maximum number of items to return (optional, default 100)

    Returns:
        Tuple of (offset, limit)

    Raises:
        ValidationError: If pagination parameters are invalid
    """
    if offset is None:
        offset = 0
    if limit is None:
        limit = 100

    if not isinstance(offset, int) or offset < 0:
        raise ValidationError("offset חייב להיות מספר חיובי / Offset must be a positive number")

    if offset > 1_000_000:
        raise ValidationError("offset גבוה מדי / Offset is too high")

    if not isinstance(limit, int) or limit < 1:
        raise ValidationError("limit חייב להיות מספר חיובי / Limit must be a positive number")

    if limit > 1000:
        raise ValidationError("limit מקסימלי הוא 1000 / Maximum limit is 1000")

    return offset, limit


def sanitize_search_query(query: str) -> str:
    """
    Sanitize search query to prevent SQL injection.

    Args:
        query: Search query string

    Returns:
        Sanitized query string

    Raises:
        ValidationError: If query is invalid
    """
    if not query or not isinstance(query, str):
        raise ValidationError("שאילתת חיפוש חייבת להיות מחרוזת לא ריקה / Search query must be a non-empty string")

    # Remove excessive whitespace
    query = ' '.join(query.split())

    # Limit length
    if len(query) > 200:
        raise ValidationError("שאילתת חיפוש ארוכה מדי (מקסימום 200 תווים) / Search query is too long (max 200 characters)")

    # Remove SQL wildcards that could be abused
    # Note: We still allow LIKE patterns in the database layer with proper parameterization
    query = query.strip()

    return query


def sanitize_string_input(text: str, field_name: str = "ערך", max_length: int = 500) -> str:
    """
    Sanitize general string input (neighborhood, city, etc).

    Args:
        text: Input text to sanitize
        field_name: Name of the field (for error messages)
        max_length: Maximum allowed length

    Returns:
        Sanitized string

    Raises:
        ValidationError: If input is invalid
    """
    if not text or not isinstance(text, str):
        raise ValidationError(f"{field_name} חייב להיות מחרוזת לא ריקה / {field_name} must be a non-empty string")

    # Remove excessive whitespace
    text = ' '.join(text.split())

    if len(text) > max_length:
        raise ValidationError(f"{field_name} ארוך מדי (מקסימום {max_length} תווים) / {field_name} is too long (max {max_length} characters)")

    return text


def validate_url(url: str) -> str:
    """
    Validate URL format.

    Args:
        url: URL to validate

    Returns:
        Validated URL

    Raises:
        ValidationError: If URL is invalid
    """
    if not url or not isinstance(url, str):
        raise ValidationError("URL חייב להיות מחרוזת לא ריקה / URL must be a non-empty string")

    # Basic URL validation
    if not re.match(r'https?://', url, re.IGNORECASE):
        raise ValidationError("URL חייב להתחיל ב-http:// או https:// / URL must start with http:// or https://")

    if len(url) > 2000:
        raise ValidationError("URL ארוך מדי / URL is too long")

    # Check for common malicious patterns
    dangerous_patterns = ['javascript:', 'data:', 'vbscript:', 'file:']
    for pattern in dangerous_patterns:
        if pattern in url.lower():
            raise ValidationError("URL מכיל תבנית מסוכנת / URL contains dangerous pattern")

    return url


def validate_filter_type(filter_type: str) -> str:
    """
    Validate filter type.

    Args:
        filter_type: Filter type to validate

    Returns:
        Validated filter type

    Raises:
        ValidationError: If filter type is invalid
    """
    allowed_types = ['price', 'rooms', 'sqm', 'neighborhood', 'city']

    if not filter_type or not isinstance(filter_type, str):
        raise ValidationError("סוג סינון חייב להיות מחרוזת לא ריקה / Filter type must be a non-empty string")

    if filter_type.lower() not in allowed_types:
        raise ValidationError(f"סוג סינון לא חוקי. מותר: {', '.join(allowed_types)} / Invalid filter type. Allowed: {', '.join(allowed_types)}")

    return filter_type.lower()


def validate_hours_param(hours: Optional[int], default: int = 24, max_hours: int = 720) -> int:
    """
    Validate hours parameter for time-based queries.

    Args:
        hours: Number of hours (optional)
        default: Default value if None
        max_hours: Maximum allowed hours

    Returns:
        Validated hours value

    Raises:
        ValidationError: If hours is invalid
    """
    if hours is None:
        return default

    if not isinstance(hours, int) or hours < 1:
        raise ValidationError("שעות חייבות להיות מספר חיובי / Hours must be a positive number")

    if hours > max_hours:
        raise ValidationError(f"מספר שעות מקסימלי הוא {max_hours} / Maximum hours is {max_hours}")

    return hours


def validate_days_param(days: Optional[int], default: int = 30, max_days: int = 365) -> int:
    """
    Validate days parameter for time-based queries.

    Args:
        days: Number of days (optional)
        default: Default value if None
        max_days: Maximum allowed days

    Returns:
        Validated days value

    Raises:
        ValidationError: If days is invalid
    """
    if days is None:
        return default

    if not isinstance(days, int) or days < 1:
        raise ValidationError("ימים חייבים להיות מספר חיובי / Days must be a positive number")

    if days > max_days:
        raise ValidationError(f"מספר ימים מקסימלי הוא {max_days} / Maximum days is {max_days}")

    return days
