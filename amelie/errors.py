class Warning(Exception):
    """
    Raised for important warnings that are not considered errors.
    Warnings indicate conditions that may not stop program execution,
    but could lead to unexpected behavior if ignored.

    Examples:
    - Data truncation during insertion
    - Deprecated API usage
    - Non-critical configuration mismatches

    Unlike Error and its subclasses, Warning does not represent a failure
    of the database or interface. It is intended to alert developers or
    users about potential issues that should be reviewed but do not
    necessarily require exception handling logic.
    """

    pass


class Error(Exception):
    """
    Base class for all error exceptions in the database API.
    This class allows developers to catch all database-related errors
    with a single `except Error:` clause.

    Notes:
    - Warnings are not considered errors and should not inherit from this class.
    - Subclasses of Error provide more specific categories of failures,
      enabling fine-grained exception handling.

    Use this class when you want to handle any error generically,
    without distinguishing between interface, database, or specific
    operational issues.
    """

    pass


class InterfaceError(Error):
    """
    Raised for errors related to the database interface rather than
    the database engine itself. These errors typically occur when
    the client-side API or driver is misused or encounters problems.

    Examples:
    - Invalid connection parameters
    - Misconfigured driver settings
    - Incorrect use of connection or cursor objects

    This exception helps separate issues caused by the interface layer
    (client code, drivers, adapters) from those originating inside
    the database engine.
    """

    pass


class DatabaseError(Error):
    """
    Base class for all errors that originate from the database engine.
    These errors indicate failures reported by the database itself,
    rather than the interface or client code.

    Subclasses of DatabaseError provide more specific categories:
    - OperationalError: Environmental or resource-related issues
    - ProgrammingError: Developer mistakes in SQL/API usage
    - IntegrityError: Violations of relational constraints
    - DataError: Problems with values or processed data
    - NotSupportedError: Unsupported operations or features
    - InternalError: Internal database state problems

    Use this class when you want to catch any database-originated
    error without distinguishing between specific categories.
    """

    pass


class OperationalError(DatabaseError):
    """
    Raised for errors related to the database’s operation, often
    outside the programmer’s direct control. These errors usually
    indicate environmental or resource-related problems.

    Examples:
    - Unexpected disconnects from the server
    - Data source not found
    - Transaction aborted due to resource limits
    - Memory allocation errors during query execution

    This exception is commonly used in retry logic, reconnection
    strategies, or failover handling.
    """

    pass


class ProgrammingError(DatabaseError):
    """
    Raised when the programmer makes a mistake in using the database API
    or SQL syntax. These errors are deterministic and usually indicate
    incorrect code rather than runtime conditions.

    Examples:
    - Table not found or already exists
    - Syntax error in SQL statements
    - Wrong number of parameters supplied
    - Invalid column name or type mismatch

    This exception helps identify and correct developer mistakes
    during query construction or API usage.
    """

    pass


class IntegrityError(DatabaseError):
    """
    Raised when relational integrity constraints are violated.
    These errors occur when schema-defined rules are not respected.

    Examples:
    - Foreign key constraint violation
    - Unique constraint violation (duplicate key)
    - Check constraint failure

    This exception is critical for enforcing business rules and
    maintaining consistent, valid data across tables.
    """

    pass


class DataError(DatabaseError):
    """
    Raised when problems occur with the data being processed.
    These errors are caused by invalid, malformed, or out-of-range
    values rather than structural or operational issues.

    Examples:
    - Division by zero in a query
    - Numeric overflow or underflow
    - Invalid string encoding
    - Malformed input data

    This exception is useful for detecting and handling cases where
    the *content* of the data is the root cause of the failure.
    """

    pass


class NotSupportedError(DatabaseError):
    """
    Raised when a database method or API call is invoked that the
    database does not support.

    Examples:
    - Calling .rollback() on a connection without transaction support
    - Using a disabled feature in the current configuration
    - Requesting unsupported SQL extensions

    This exception signals that the attempted operation is fundamentally
    incompatible with the database’s capabilities.
    """

    pass


class InternalError(DatabaseError):
    """
    Raised when the database encounters an internal problem,
    often indicating corruption or a bug within the database engine.

    Examples:
    - Cursor becomes invalid or unusable
    - Transaction state is inconsistent
    - Low-level database malfunction

    These errors are typically not recoverable by application logic
    and may require restarting the connection or reporting the issue
    to the database vendor.
    """

    pass
