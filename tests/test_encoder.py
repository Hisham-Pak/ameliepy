import json
import uuid
from datetime import date, time, datetime
from decimal import Decimal

import pytest

import amelie.errors as e
import amelie._encoder as encoder


def test_literal_none():
    assert encoder.literal(None) == "NULL"


def test_literal_bool():
    assert encoder.literal(True) == "TRUE"
    assert encoder.literal(False) == "FALSE"


def test_literal_integers_and_bigints():
    assert encoder.literal(1) == "1"
    assert encoder.literal(0) == "0"
    assert encoder.literal(-42) == "-42"
    # Big int
    big = 2**62
    assert encoder.literal(big) == str(big)


def test_literal_floats_and_special():
    assert encoder.literal(3.14) == "3.14"
    nan_lit = encoder.literal(float("nan"))
    assert nan_lit.startswith("'") and nan_lit.endswith("'")
    inf_lit = encoder.literal(float("inf"))
    assert inf_lit.startswith("'") and inf_lit.endswith("'")


def test_literal_string_and_escaping():
    assert encoder.literal("a") == "'a'"
    assert encoder.literal("a'b") == "'a''b'"  # single quote doubled


def test_literal_json_like():
    v = {"x": 1, "y": [1, 2]}
    lit = encoder.literal(v)
    # should be a quoted JSON string
    assert lit.startswith("'") and lit.endswith("'")
    inner = lit[1:-1]
    assert json.loads(inner) == v


def test_literal_uuid_and_binary():
    u = uuid.uuid4()
    assert encoder.literal(u) == "'" + str(u) + "'"

    b = b"\x01\x02"
    with pytest.raises(e.NotSupportedError):
        encoder.literal(b)


def test_literal_date_time_timestamp():
    d = date(2020, 1, 2)
    assert encoder.literal(d) == "'2020-01-02'"

    t = time(3, 4, 5)
    assert encoder.literal(t) == "'03:04:05'"

    ts = datetime(2020, 1, 2, 3, 4, 5)
    assert encoder.literal(ts).startswith("'2020-01-02 03:04:05")


def test_format_query_positional_and_named():
    q = "INSERT INTO tbl (id, val) VALUES (%s, %s)"
    out = encoder.format_query(q, (1, "a"))
    assert "VALUES (1, 'a')" in out

    q2 = "SELECT * FROM tbl WHERE id = %(id)s AND name = %(name)s"
    out2 = encoder.format_query(q2, {"id": 5, "name": "o'neil"})
    assert "id = 5" in out2
    assert "name = 'o''neil'" in out2


def test_format_query_mismatch_raises():
    with pytest.raises(e.ProgrammingError):
        encoder.format_query("INSERT INTO t (a,b) VALUES (%s, %s)", (1,))

    with pytest.raises(e.ProgrammingError):
        encoder.format_query(
            "SELECT * FROM t WHERE id = %(id)s and name = %(name)s", {"id": 1}
        )


def test_mapping_styles():
    # Format a query and pretend server returns the same values as JSON
    sql = encoder.format_query("INSERT INTO t (id, val) VALUES (%s, %s)", (1, "o'k"))
    assert sql == "INSERT INTO t (id, val) VALUES (1, 'o''k')"

    sql = encoder.format_query(
        "INSERT INTO t (id, val) VALUES (%(id)s, %(val)s)", {"id": 2, "val": "test"}
    )
    assert sql == "INSERT INTO t (id, val) VALUES (2, 'test')"

@pytest.mark.parametrize(
    "query, params, expected_sql",
    [
        ("SELECT %(bool1)s, %(bool2)s", {"bool1": True, "bool2": False}, "SELECT TRUE, FALSE"),
        ("SELECT %(int1)s, %(int2)s", {"int1": 1, "int2": 0}, "SELECT 1, 0"),
        ("SELECT %(str1)s, %(str2)s", {"str1": "true", "str2": "false"}, "SELECT 'true', 'false'"),
        ("SELECT %(null1)s, %(null2)s", {"null1": None, "null2": "NULL"}, "SELECT NULL, 'NULL'"),
        ("SELECT %(float1)s, %(float2)s", {"float1": 3.14, "float2": -2.71}, "SELECT 3.14, -2.71"),
        # integer sizes
        (
            "SELECT %(i1)s, %(i2)s, %(i3)s, %(i4)s, %(i5)s",
            {"i1": 127, "i2": -128, "i3": 32767, "i4": 2147483647, "i5": 9223372036854775807},
            "SELECT 127, -128, 32767, 2147483647, 9223372036854775807",
        ),
        # float / Decimal
        ("SELECT %(f1)s, %(f2)s", {"f1": 1.5, "f2": Decimal(2.25)}, "SELECT 1.5, 2.25"),
        # text
        ("SELECT %(s1)s, %(s2)s", {"s1": "hello", "s2": "world"}, "SELECT 'hello', 'world'"),
        # json
        ("SELECT %(j1)s", {"j1": {"a": 1, "b": "x"}}, 'SELECT \'{"a":1,"b":"x"}\''),
        ("SELECT %(j2)s", {"j2": [1, 2, 3]}, "SELECT '[1,2,3]'"),
        # date / timestamp
        (
            "SELECT DATE %(date)s, TIMESTAMP %(ts)s::at_timezone('UTC')",
            {"date": date.fromisoformat("2020-01-01"), "ts": datetime.fromisoformat("2020-01-01 07:34:56")},
            "SELECT DATE '2020-01-01', TIMESTAMP '2020-01-01 07:34:56'::at_timezone('UTC')",
        ),
        # uuid
        (
            "SELECT UUID %(uuid)s",
            {"uuid": uuid.UUID("550e8400-e29b-41d4-a716-446655440000")},
            "SELECT UUID '550e8400-e29b-41d4-a716-446655440000'",
        ),
        # vector
        ("SELECT VECTOR %(vec)s", {"vec": [1.2, 2.3]}, "SELECT VECTOR '[1.2,2.3]'")
    ],
)
def test_format_query_various_literals_mapping_style(query, params, expected_sql):
    sql = encoder.format_query(query, params)
    assert sql == expected_sql

@pytest.mark.parametrize(
    "query, params, expected_sql",
    [
        ("SELECT %s, %s", [True, False], "SELECT TRUE, FALSE"),
        ("SELECT %s, %s", [1, 0], "SELECT 1, 0"),
        ("SELECT %s, %s", ["true", "false"], "SELECT 'true', 'false'"),
        ("SELECT %s, %s", [None, "NULL"], "SELECT NULL, 'NULL'"),
        ("SELECT %s, %s", [3.14, -2.71], "SELECT 3.14, -2.71"),
        # integer sizes
        (
            "SELECT %s, %s, %s, %s, %s",
            [127, -128, 32767, 2147483647, 9223372036854775807],
            "SELECT 127, -128, 32767, 2147483647, 9223372036854775807",
        ),
        # float / double
        ("SELECT %s, %s", [1.5, 2.25], "SELECT 1.5, 2.25"),
        # text
        ("SELECT %s, %s", ["hello", "world"], "SELECT 'hello', 'world'"),
        # json
        ("SELECT %s", [{"a": 1, "b": "x"}], 'SELECT \'{"a":1,"b":"x"}\''),
        ("SELECT %s", [[1, 2, 3]], "SELECT '[1,2,3]'"),
        # date / timestamp
        (
            "SELECT DATE %s, TIMESTAMP %s::at_timezone('UTC')",
            [
                date.fromisoformat("2020-01-01"),
                datetime.fromisoformat("2020-01-01 07:34:56"),
            ],
            "SELECT DATE '2020-01-01', TIMESTAMP '2020-01-01 07:34:56'::at_timezone('UTC')",
        ),
        # uuid
        (
            "SELECT UUID %s",
            [uuid.UUID("550e8400-e29b-41d4-a716-446655440000")],
            "SELECT UUID '550e8400-e29b-41d4-a716-446655440000'",
        ),
        # vector
        ("SELECT VECTOR %s", [[1.2, 2.3]], "SELECT VECTOR '[1.2,2.3]'"),
    ],
)
def test_format_query_various_literals_sequence_style(query, params, expected_sql):
    sql = encoder.format_query(query, params)
    assert sql == expected_sql
