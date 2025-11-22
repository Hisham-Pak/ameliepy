"""DB-API 2.0 type constructors for Amelie.

This module contains DB-API style constructors (Date, Time, Timestamp, Binary)
and sentinel type objects.

https://peps.python.org/pep-0249/#type-objects-and-constructors
"""

from datetime import date as _date, time as _time, datetime as _datetime

# DB-API Type objects (sentinels)
STRING = object()
BINARY = object()
NUMBER = object()
DATETIME = object()
ROWID = object()


class Date:
    def __init__(self, year, month, day):
        self._d = _date(year, month, day)

    def __str__(self):
        return self._d.isoformat()


class Time:
    def __init__(self, hour, minute, second):
        self._t = _time(hour, minute, second)

    def __str__(self):
        return self._t.isoformat()


class Timestamp:
    def __init__(self, year, month, day, hour, minute, second):
        self._dt = _datetime(year, month, day, hour, minute, second)

    def __str__(self):
        return self._dt.isoformat(sep=" ")


def DateFromTicks(ticks):
    dt = _datetime.fromtimestamp(ticks)
    return Date(dt.year, dt.month, dt.day)


def TimeFromTicks(ticks):
    dt = _datetime.fromtimestamp(ticks)
    return Time(dt.hour, dt.minute, dt.second)


def TimestampFromTicks(ticks):
    dt = _datetime.fromtimestamp(ticks)
    return Timestamp(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)


class Binary:
    """Wrapper for binary data to indicate BINARY type.

    The content is stored as bytes. literal() will render it as a hex
    literal (X'...') which is a common SQL notation for binary data.
    """

    def __init__(self, value: bytes):
        if not isinstance(value, (bytes, bytearray)):
            raise TypeError("Binary() requires bytes or bytearray")
        self._b = bytes(value)

    def __bytes__(self):
        return self._b
