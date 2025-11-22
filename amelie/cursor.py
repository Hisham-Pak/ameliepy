from . import errors as e
from ._encoder import format_query
from ._decoder import from_ameliedb_value
from ._protocol import Protocol


class Cursor(Protocol):
    def __init__(self, connection):
        self.connection = connection
        self._results = None  # Store the results of the last executed query
        self._row_index = 0  # Index to track the current row in results to fetch
        self.arraysize = 1  # Default fetchmany size
        self.closed = False
        super().__init__(self.connection)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False

    def execute(self, query, params=None):
        if self.closed or self.connection.closed:
            raise e.ProgrammingError("Connection or cursor is closed.")
        # Safely bind parameters into the query using format_query which
        # converts Python values into properly quoted SQL literals.
        try:
            sql = format_query(query, params)
        except Exception as err:
            if isinstance(err, e.ProgrammingError):
                raise
            raise e.ProgrammingError(str(err))
        self._results = from_ameliedb_value(self.send_request(sql))
        self._row_index = 0

    def fetchone(self):
        if self._results and self._row_index < len(self._results):
            row = self._results[self._row_index]
            self._row_index += 1
            return row
        return None

    def fetchmany(self, size=None):
        size = self.arraysize if size is None else size
        if not self._results:
            return []
        start = self._row_index
        end = min(start + size, len(self._results))
        rows = self._results[start:end]
        self._row_index = end
        return rows

    def fetchall(self):
        if self._results:
            rows = self._results[self._row_index :]
            self._row_index = len(self._results)
            return rows
        return []

    def close(self):
        self.closed = True

    @property
    def description(self):
        """
        This read-only attribute is a sequence of 7-item sequences.
        Each of these sequences contains information describing one result column:

        name: the name of the column
        type_code: the type code of the column matching the DB-API type codes
        display_size: the actual length of the column in characters
        internal_size: the size in bytes of the column
        precision: total number of significant digits for numeric types
        scale: number of digits to the right of the decimal point for numeric types
        null_ok: whether the column can accept null values

        Returns None if no query has been executed yet.
        """
        # TODO: implement description properly
        # Since amelie does not implement column metadata retrieval as it does not require
        # any client library, this method always returns None.
        if self._results is None:
            return None
        return None

    @property
    def rowcount(self):
        """This read-only attribute specifies the number of rows that the last .execute*() produced."""
        return len(self._results) if self._results is not None else -1

    def callproc(self, procname, parameters=None):
        """Call a stored database procedure with the given name."""
        # no-op
        pass

    def setinputsizes(self, sizes):
        """Predefine memory areas for the operation’s parameters."""
        # no-op
        pass

    def setoutputsize(self, size, column=None):
        """Set a column buffer size for fetches of large columns."""
        # no-op
        pass
