from typing import Literal
from datetime import date

def build_url(base_url, params=None, list_handling: Literal["collapse", "first", "none"] = "collapse"):
    """
    Builds a URL with query parameters.

    :param base_url: The base URL.
    :param params: Dictionary of query parameters.
    :param list_handling: How to handle list/tuple values:
                          - "collapse" → Join as comma-separated string (default)
                          - "first" → Use only the first value
                          - "none" → Keep as tuple/list (not recommended for URLs)
    :return: Formatted URL string.
    """
    full_url = base_url.rstrip("/")  # Remove trailing slash
    if not params:
        return full_url

    formatted_params = {}
    for key, value in params.items():
        # Convert dates to ISO format (YYYY-MM-DD)
        if isinstance(value, date):
            value = value.isoformat()
        # Convert tuples to lists for consistency
        elif isinstance(value, tuple):
            value = list(value)
        # Handle lists/tuples according to `list_handling`
        if isinstance(value, list):
            if list_handling == "collapse":
                value = ",".join(value) if value else None  # Remove empty lists
            elif list_handling == "first":
                value = value[0] if value else None  # Remove empty lists

        if value:  # Skip empty values (None, "", [], ())
            formatted_params[key] = value

    # Build query string
    query_string = "&".join(f"{key}={value}" for key, value in formatted_params.items())

    return f"{full_url}?{query_string}" if query_string else full_url