"""SQL literal encoder.

This module contains functions that convert Python values into safe SQL
literal strings.
"""

from datetime import date as _date, time as _time, datetime as _datetime
import json as _json
import uuid as _uuid
from typing import Any
from decimal import Decimal

from . import errors as e
from .types import Date, Time, Timestamp, Binary


def _escape_string(s: str) -> str:
    # Double single-quotes per SQL standard
    return s.replace("'", "''")


def literal(value: Any) -> str:
    """Convert a Python value to a safe Amelie SQL literal string.

    Rules:
    - None -> NULL
    - bool -> TRUE/FALSE
    - int/float/Decimal -> literal numeric
    - str -> single-quoted with internal single-quotes doubled
    - dict/list/tuple -> JSON string (quoted)
    - Date/Time/Timestamp/_date/_time/_datetime -> formatted and quoted
    - uuid.UUID -> quoted string
    """
    # None
    if value is None:
        return "NULL"

    # Boolean
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    # Numbers
    if isinstance(value, (int, float, Decimal)):
        if isinstance(value, float):
            if (
                _json.dumps(value) == "NaN"
                or value == float("inf")
                or value == float("-inf")
            ):
                # Fallback: quote as string
                return "'" + _escape_string(str(value)) + "'"
        return str(value)

    # Strings
    if isinstance(value, str):
        return "'" + _escape_string(value) + "'"

    # JSON-like
    if isinstance(value, (dict, list, tuple)):
        return "'" + _escape_string(_json.dumps(value, separators=(",", ":"))) + "'"

    # UUID
    if isinstance(value, _uuid.UUID):
        return "'" + str(value) + "'"

    # Binary
    if isinstance(value, (bytes, bytearray, Binary)):
        raise e.NotSupportedError("Binary data type is not supported for SQL literals")

    # Date/Time/Timestamp wrappers or stdlib types
    if isinstance(value, Date):
        return "'" + _escape_string(str(value)) + "'"
    if isinstance(value, Time):
        return "'" + _escape_string(str(value)) + "'"
    if isinstance(value, Timestamp):
        return "'" + _escape_string(str(value)) + "'"
    if isinstance(value, _date) and not isinstance(value, _datetime):
        return "'" + _escape_string(value.isoformat()) + "'"
    if isinstance(value, _time):
        return "'" + _escape_string(value.isoformat()) + "'"
    if isinstance(value, _datetime):
        return "'" + _escape_string(value.isoformat(sep=" ")) + "'"

    # Fallback: try to JSON encode and quote
    try:
        s = _json.dumps(value)
        return "'" + _escape_string(s) + "'"
    except Exception:
        raise TypeError(f"Can't convert type {type(value)!r} to SQL literal")


def format_query(query: str, params):
    """Safely bind params into query.

    - For positional sequences, replace each %s with a literal.
    - For mapping, replace %(name)s with literal(params[name]).

    Raises ProgrammingError on mismatch or unsupported placeholders.
    """
    from . import errors as e

    if params is None:
        return query

    # Mapping style: %(name)s
    if isinstance(params, dict):
        out = query
        for k, v in params.items():
            placeholder = "%(" + str(k) + ")s"
            if placeholder not in out:
                # it's okay if some keys are unused; continue
                continue
            out = out.replace(placeholder, literal(v))
        # After replacement, check if any %(name)s remain
        if "%(" in out and ")s" in out:
            raise e.ProgrammingError("Not all mapping parameters were provided")
        return out

    # Sequence style: %s
    # Support any iterable (tuple/list)
    try:
        seq = list(params)
    except TypeError:
        raise e.ProgrammingError("Parameters must be a sequence or mapping")

    out = query
    count_placeholders = out.count("%s")
    if count_placeholders != len(seq):
        # allow extra params? No — be strict
        raise e.ProgrammingError(
            f"Expected {count_placeholders} parameters, got {len(seq)}"
        )

    for val in seq:
        out = out.replace("%s", literal(val), 1)

    return out
