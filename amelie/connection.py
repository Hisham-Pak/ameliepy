from .cursor import Cursor


class Connection:
    def __init__(self, host):
        self.host = host
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False

    @classmethod
    def connect(cls, host="http://localhost:3485"):
        return cls(host)

    def cursor(self):
        return Cursor(self)

    def close(self):
        self.closed = True
        # Lazy Invalidation (Cursors check parent connection state)
        # so no need to explicitly track and close all the cursors here.

    def commit(self):
        # No-op for this driver
        pass

    def rollback(self):
        # No-op for this driver
        pass
