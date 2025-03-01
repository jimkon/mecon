def build_url(base_url, params=None):
    full_url = base_url.rstrip("/")  # Remove trailing slash
    if not params:  # Handles None and empty dict
        return full_url

    query_string = "&".join(
        f"{key}={'%2C'.join(value) if isinstance(value, list) else value}"
        for key, value in params.items()
    )

    return f"{full_url}?{query_string}"
