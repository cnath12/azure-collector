"""JSON utilities for the collector."""
import json
from datetime import datetime
from enum import Enum

def to_json(obj, indent=None):
    """Convert object to JSON string."""
    return json.dumps(obj, indent=indent, default=_json_serializer)

def _json_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Enum):
        return obj.value
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')