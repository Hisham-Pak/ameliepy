import os
import pytest
import amelie
from amelie import errors as e


def test_connect():
    conn = amelie.connect()
    assert not conn.closed
    conn.close()
    assert conn.closed


def test_connection_context_manager():
    with amelie.connect() as conn:
        assert not conn.closed
    assert conn.closed


def test_connection_invalid_host():
    invalid_host = "http://invalidhost:1234"
    with pytest.raises(e.OperationalError):
        conn = amelie.connect(host=invalid_host)
        cur = conn.cursor()
        cur.execute("SELECT 1")


def test_connection_environment_variable():
    host = os.getenv("host", "http://localhost:3485")
    conn = amelie.connect()
    assert conn.host == host
    conn.close()


def test_connection_custom_host():
    custom_host = "http://customhost:5678"
    conn = amelie.connect(host=custom_host)
    assert conn.host == custom_host
    conn.close()


def test_connection_commit_rollback(db_setup):
    conn, _, _ = db_setup
    # Since commit and rollback are no-ops, just ensure they don't raise errors
    conn.commit()
    conn.rollback()


def test_connection_double_close(db_setup):
    conn, _, _ = db_setup
    conn.close()
    conn.close()
    assert conn.closed
