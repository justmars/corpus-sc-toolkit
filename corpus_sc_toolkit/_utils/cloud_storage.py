import re
from typing import Any


def get_from_prefix(client, bucket_name: str, key: str):
    """A try/except block is needed since a `NoKeyFound` exception is raised
    when a retrieval is made without a result."""
    try:
        return client.get_object(Bucket=bucket_name, Key=key)
    except Exception:
        return None
