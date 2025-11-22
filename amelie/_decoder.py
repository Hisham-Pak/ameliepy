"""Decode AmelieDB responses into Python types.

This module provides conservative, deterministic conversion functions that
prefer server-provided type metadata when available and otherwise fall back
to safe inference.
"""

from datetime import date as _date, datetime as _datetime
import json as _json
import uuid as _uuid
from typing import Any


def from_ameliedb_value(value: Any, type_code: str | None = None):
    """Convert a single Amelie value to an appropriate Python object.

    e.g https://amelielabs.io/docs/sql/types/bool/

    - If type_code is provided, conversion follows that type.
    - Otherwise, perform conservative inference (JSON, scalars, UUID).
    """
    # Prefer explicit None
    if value is None:
        return None

    if type_code:
        # NOTE: These will not work until we have proper type metadata from server
        # and Columns are implemented in cursor.description.
        tc = str(type_code).upper()
        if tc == "BOOL":
            return bool(value)
        if tc in ("TINYINT", "SMALLINT", "INT", "BIGINT"):
            try:
                return int(value)
            except Exception:
                return int(float(value))
        if tc in ("FLOAT", "DOUBLE"):
            return float(value)
        if tc == "TEXT":
            return str(value)
        if tc == "JSON":
            try:
                return _json.loads(value) if isinstance(value, str) else value
            except Exception:
                return value
        if tc == "TIMESTAMP":
            if isinstance(value, str):
                try:
                    return _datetime.fromisoformat(value)
                except Exception:
                    return value
            return value
        if tc == "DATE":
            if isinstance(value, str):
                try:
                    return _date.fromisoformat(value)
                except Exception:
                    return value
            return value
        if tc == "VECTOR":
            # Expect a JSON array in string form or a list
            if isinstance(value, str):
                try:
                    return _json.loads(value)
                except Exception:
                    return value
            if isinstance(value, list):
                return value
            return value
        if tc == "UUID":
            try:
                return _uuid.UUID(value) if isinstance(value, str) else value
            except Exception:
                return value
        if tc == "NULL":
            return None

    # No type_code: conservative inference
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, list):
        return [from_ameliedb_value(v) for v in value]
    if isinstance(value, dict):
        return {k: from_ameliedb_value(v) for k, v in value.items()}
    if isinstance(value, str):
        s = value.strip()
        if s.startswith("{") or s.startswith("["):
            try:
                return _json.loads(s)
            except Exception:
                pass
        if len(s) == 36 and s.count("-") == 4:
            try:
                return _uuid.UUID(s)
            except Exception:
                pass
        try:
            if "T" in s or ":" in s:
                return _datetime.fromisoformat(s)
            return _date.fromisoformat(s)
        except Exception:
            pass
        return value

    return value
