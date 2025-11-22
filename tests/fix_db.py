import os
import pytest
import amelie


@pytest.fixture()
def db_setup():
    host = os.getenv("host", "http://localhost:3485")
    schema = os.getenv("schema", "ameliepy_test")

    # Setup
    conn = amelie.connect(host=host)
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}.test_table (id int primary key, val text)"
    )

    yield conn, cur, schema

    # Teardown
    if not cur.closed and not conn.closed:
        # Some tests may already close the cursor/connection
        cur.execute(f"DROP TABLE IF EXISTS {schema}.test_table")
        cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        cur.close()
        conn.close()
