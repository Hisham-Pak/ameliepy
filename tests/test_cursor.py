import pytest
from amelie import errors as e


def test_hello_world(db_setup):
    _, cur, _ = db_setup
    cur.execute("SELECT 'Hello, World!'")
    row = cur.fetchone()
    # cursor returns a mapping of column names to values
    assert row["col1"] == "Hello, World!"


def test_insert_and_query(db_setup):
    _, cur, schema = db_setup
    cur.execute(f"INSERT INTO {schema}.test_table (id, val) VALUES (1, 'a'), (2, 'b')")
    cur.execute(f"SELECT id, val FROM {schema}.test_table ORDER BY id")
    rows = cur.fetchall()
    assert rows == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]


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
    assert row == {"id": 1, "val": "a"}


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
    assert row1 == {"id": 1, "val": "a"}
    assert row2 == {"id": 2, "val": "b"}
    assert row3 is None


def test_fetchmany(db_setup):
    _, cur, schema = db_setup
    cur.execute(
        f"INSERT INTO {schema}.test_table (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    )
    cur.execute(f"SELECT id, val FROM {schema}.test_table ORDER BY id")
    rows = cur.fetchmany(size=2)
    assert rows == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    rows = cur.fetchmany(size=2)
    assert rows == [{"id": 3, "val": "c"}]


def test_fetchall(db_setup):
    _, cur, schema = db_setup
    cur.execute(f"INSERT INTO {schema}.test_table (id, val) VALUES (1, 'a'), (2, 'b')")
    cur.execute(f"SELECT id, val FROM {schema}.test_table ORDER BY id")
    rows = cur.fetchall()
    assert rows == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]


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
    # description should now be populated with (name, type_code, display_size,
    # internal_size, precision, scale, null_ok) tuples for each column
    assert desc is not None
    assert len(desc) == 2
    # id should be an integer type, val should be text
    from amelie.FIELD_TYPE import FIELD_MAP

    assert desc[0][0] == "id"
    assert desc[0][1] == FIELD_MAP.get("INT")
    assert desc[1][0] == "val"
    assert desc[1][1] == FIELD_MAP.get("TEXT")


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
        assert row == {"id": 1, "val": "a"}
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
        assert row == {"id": 1, "val": "a"}


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
    assert row == {"id": 1, "val": "a"}
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
    assert row == {"id": 1, "val": "a"}


def test_cursor_select_all(db_setup):
    _, cur, schema = db_setup
    cur.execute(
        f"INSERT INTO {schema}.test_table (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    )
    cur.execute(f"SELECT * FROM {schema}.test_table ORDER BY id")
    rows = cur.fetchall()
    assert rows == [
        {"id": 1, "val": "a"},
        {"id": 2, "val": "b"},
        {"id": 3, "val": "c"},
    ]

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
