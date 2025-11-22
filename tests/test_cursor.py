import pytest
from amelie import errors as e


def test_hello_world(db_setup):
    _, cur, _ = db_setup
    cur.execute("SELECT 'Hello, World!'")
    row = cur.fetchone()
    assert row == "Hello, World!"


def test_insert_and_query(db_setup):
    _, cur, schema = db_setup
    cur.execute(f"INSERT INTO {schema}.test_table (id, val) VALUES (1, 'a'), (2, 'b')")
    cur.execute(f"SELECT id, val FROM {schema}.test_table ORDER BY id")
    rows = cur.fetchall()
    assert rows == [[1, "a"], [2, "b"]]


def test_invalid_query(db_setup):
    _, cur, schema = db_setup
    with pytest.raises(e.ProgrammingError):
        cur.execute("SELECT * FROM non_existent_table")


def test_execute_with_params(db_setup):
    _, cur, schema = db_setup
    # TODO: convert to supported datatypes
    cur.execute(f"INSERT INTO {schema}.test_table (id, val) VALUES (%s, %s)", (1, "a"))
    cur.execute(f"SELECT id, val FROM {schema}.test_table WHERE id = %s", (1,))
    row = cur.fetchone()
    assert row == [1, "a"]


def test_execute_after_close(db_setup):
    _, cur, schema = db_setup
    cur.close()
    with pytest.raises(e.ProgrammingError):
        cur.execute(f"SELECT id, val FROM {schema}.test_table")


def test_execute_on_closed_connection(db_setup):
    conn, cur, schema = db_setup
    conn.close()
    with pytest.raises(e.ProgrammingError):
        cur.execute(f"SELECT id, val FROM {schema}.test_table")


def test_execute_no_results(db_setup):
    _, cur, schema = db_setup
    cur.execute(f"SELECT id, val FROM {schema}.test_table WHERE id = -1")
    rows = cur.fetchall()
    assert rows == []


def test_fetchone(db_setup):
    _, cur, schema = db_setup
    cur.execute(f"INSERT INTO {schema}.test_table (id, val) VALUES (1, 'a'), (2, 'b')")
    cur.execute(f"SELECT id, val FROM {schema}.test_table ORDER BY id")
    row1 = cur.fetchone()
    row2 = cur.fetchone()
    row3 = cur.fetchone()
    assert row1 == [1, "a"]
    assert row2 == [2, "b"]
    assert row3 is None


def test_fetchmany(db_setup):
    _, cur, schema = db_setup
    cur.execute(
        f"INSERT INTO {schema}.test_table (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    )
    cur.execute(f"SELECT id, val FROM {schema}.test_table ORDER BY id")
    rows = cur.fetchmany(size=2)
    assert rows == [[1, "a"], [2, "b"]]
    rows = cur.fetchmany(size=2)
    assert rows == [[3, "c"]]


def test_fetchall(db_setup):
    _, cur, schema = db_setup
    cur.execute(f"INSERT INTO {schema}.test_table (id, val) VALUES (1, 'a'), (2, 'b')")
    cur.execute(f"SELECT id, val FROM {schema}.test_table ORDER BY id")
    rows = cur.fetchall()
    assert rows == [[1, "a"], [2, "b"]]


def test_fetch_no_results(db_setup):
    _, cur, schema = db_setup
    cur.execute(f"SELECT id, val FROM {schema}.test_table WHERE id = -1")
    row = cur.fetchone()
    assert row is None
    rows = cur.fetchmany(size=5)
    assert rows == []
    all_rows = cur.fetchall()
    assert all_rows == []


def test_description(db_setup):
    _, cur, schema = db_setup
    cur.execute(f"INSERT INTO {schema}.test_table (id, val) VALUES (1, 'a')")
    cur.execute(f"SELECT id, val FROM {schema}.test_table")
    desc = cur.description
    assert desc is None  # TODO: Since description is not yet implemented


def test_rowcount(db_setup):
    _, cur, schema = db_setup
    cur.execute(f"INSERT INTO {schema}.test_table (id, val) VALUES (1, 'a'), (2, 'b')")
    assert cur.rowcount == 0
    cur.execute(f"SELECT id, val FROM {schema}.test_table")
    assert cur.rowcount == 2
    cur.execute(f"SELECT id, val FROM {schema}.test_table WHERE id = 1")
    assert cur.rowcount == 1


def test_cursor_close(db_setup):
    _, cur, schema = db_setup
    cur.close()
    assert cur.closed
    with pytest.raises(e.ProgrammingError):
        cur.execute(f"SELECT id, val FROM {schema}.test_table")


def test_cursor_double_close(db_setup):
    _, cur, _ = db_setup
    cur.close()
    cur.close()
    assert cur.closed


def test_cursor_context_manager(db_setup):
    conn, _, schema = db_setup
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO {schema}.test_table (id, val) VALUES (1, 'a')")
        cur.execute(f"SELECT id, val FROM {schema}.test_table")
        row = cur.fetchone()
        assert row == [1, "a"]
    assert cur.closed


def test_cursor_context_manager_closes_on_exception(db_setup):
    conn, _, schema = db_setup
    try:
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO {schema}.test_table (id, val) VALUES (1, 'a')")
            raise ValueError("Intentional Error")
    except ValueError:
        pass
    assert cur.closed

    # Ensure the data was still inserted
    with conn.cursor() as cur:
        cur.execute(f"SELECT id, val FROM {schema}.test_table")
        row = cur.fetchone()
        assert row == [1, "a"]


def test_cursor_reuse_after_close(db_setup):
    conn, _, schema = db_setup
    cur = conn.cursor()
    cur.close()
    with pytest.raises(e.ProgrammingError):
        cur.execute(f"SELECT id, val FROM {schema}.test_table")


def test_cursor_multiple(db_setup):
    conn, _, schema = db_setup
    cur1 = conn.cursor()
    cur2 = conn.cursor()
    cur1.execute(f"INSERT INTO {schema}.test_table (id, val) VALUES (1, 'a')")
    cur2.execute(f"SELECT id, val FROM {schema}.test_table")
    row = cur2.fetchone()
    assert row == [1, "a"]
    cur1.close()
    cur2.close()


def test_sudden_db_server_disconnect(db_setup):
    _, cur, schema = db_setup
    # Simulate by using an invalid host temporarily
    original_host = cur.connection.host
    cur.connection.host = "http://invalidhost:1234"
    with pytest.raises(e.OperationalError):
        cur.execute(f"SELECT id, val FROM {schema}.test_table")
    # Restore original host
    cur.connection.host = original_host
    # Ensure cursor is still usable
    cur.execute(f"INSERT INTO {schema}.test_table (id, val) VALUES (1, 'a')")
    cur.execute(f"SELECT id, val FROM {schema}.test_table")
    row = cur.fetchone()
    assert row == [1, "a"]


def test_cursor_select_all(db_setup):
    _, cur, schema = db_setup
    cur.execute(
        f"INSERT INTO {schema}.test_table (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    )
    cur.execute(f"SELECT * FROM {schema}.test_table ORDER BY id")
    rows = cur.fetchall()
    assert rows == [[1, "a"], [2, "b"], [3, "c"]]

def test_cursor_row_count(db_setup):
    _, cur, schema = db_setup
    cur.execute(
        f"INSERT INTO {schema}.test_table (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    )
    cur.execute(f"SELECT * FROM {schema}.test_table")
    assert cur.rowcount == 3

    cur.execute(f"SELECT * FROM {schema}.test_table WHERE id = -1")
    assert cur.rowcount == 0

    cur.execute(f"INSERT INTO {schema}.test_table (id, val) VALUES (4, 'd')")
    assert cur.rowcount == 0  # INSERT does not affect rowcount
