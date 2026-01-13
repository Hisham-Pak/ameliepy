"""
Amelie DB-API v2.0 driver

Example usage:

```
import amelie

with amelie.connect(host="http://localhost:3485") as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1")
        row = cursor.fetchone()
        print(row)
        # Will output: 1
```
"""

from .connection import Connection
from .cursor import Cursor
from .errors import (
    Warning,
    Error,
    InterfaceError,
    DatabaseError,
    OperationalError,
    ProgrammingError,
    IntegrityError,
    DataError,
    NotSupportedError,
    InternalError,
)
from .types import (
    Date,
    Time,
    Timestamp,
    Binary,
    STRING,
    BINARY,
    NUMBER,
    DATETIME,
    ROWID,
    DateFromTicks,
    TimeFromTicks,
    TimestampFromTicks,
)
from .FIELD_TYPE import FIELD_MAP

# DBAPI compliance
connect = Connection.connect
apilevel = "2.0"
threadsafety = 1
paramstyle = "pyformat"

# Package metadata
__version__ = "0.2.1"

__all__ = [
    # Core components
    "connect",
    "Connection",
    "Cursor",
    # Errors
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "OperationalError",
    "ProgrammingError",
    "IntegrityError",
    "DataError",
    "NotSupportedError",
    "InternalError",
    # Types
    "Date",
    "Time",
    "Timestamp",
    "Binary",
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    "DateFromTicks",
    "TimeFromTicks",
    "TimestampFromTicks",
    # Field Types
    "FIELD_MAP",
]
