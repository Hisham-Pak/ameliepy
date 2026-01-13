import json
import uuid
from datetime import date, datetime

import pytest

import amelie._encoder as encoder
import amelie._decoder as decoder


def test_decoder_from_ameliedb_value_with_typecodes():
    assert decoder.from_ameliedb_value(None, "NULL") is None
    assert decoder.from_ameliedb_value(True, "BOOL") is True
    assert decoder.from_ameliedb_value("1", "INT") == 1
    assert isinstance(decoder.from_ameliedb_value("1.5", "FLOAT"), float)
    assert decoder.from_ameliedb_value("hello", "TEXT") == "hello"

    json_str = json.dumps({"a": 1})
    assert decoder.from_ameliedb_value(json_str, "JSON") == {"a": 1}

    ts = "2020-01-02 03:04:05"
    v = decoder.from_ameliedb_value(ts, "TIMESTAMP")
    assert isinstance(v, datetime)
    assert v.year == 2020 and v.hour == 3

    d = "2020-01-02"
    dv = decoder.from_ameliedb_value(d, "DATE")
    assert isinstance(dv, date) and dv.year == 2020

    u = str(uuid.uuid4())
    decoder.from_ameliedb_value(u, "UUID")
    assert isinstance(decoder.from_ameliedb_value(u, "UUID"), uuid.UUID)

    # INTERVAL - conservative: return as-is if not parseable
    assert decoder.from_ameliedb_value("1 day", "INTERVAL") == "1 day"

    # VECTOR - expect JSON parsing when provided as text
    vec = "[1,2,3]"
    assert decoder.from_ameliedb_value(vec, "VECTOR") == [1, 2, 3]


def test_decoder_inference_without_typecodes():
    assert decoder.from_ameliedb_value(1) == 1
    assert decoder.from_ameliedb_value(1.5) == 1.5
    assert decoder.from_ameliedb_value(True) is True
    assert decoder.from_ameliedb_value([1, "2", {"x": "3"}]) == [1, "2", {"x": "3"}]

    # JSON string
    s = json.dumps({"a": 1})
    assert decoder.from_ameliedb_value(s) == {"a": 1}

    # UUID inference
    u = str(uuid.uuid4())
    assert isinstance(decoder.from_ameliedb_value(u), uuid.UUID)


@pytest.mark.parametrize(
    "query, expected_data_value",
    [
        ("SELECT TRUE, FALSE", [True, False]),
        ("SELECT 1, 0", [1, 0]),
        ("SELECT 'true', 'false'", ["true", "false"]),
        ("SELECT NULL, 'NULL'", [None, "NULL"]),
        ("SELECT 3.14, -2.71", [3.14, -2.71]),
        # integer sizes
        (
            "SELECT 127, -128, 32767, 2147483647, 9223372036854775807",
            [127, -128, 32767, 2147483647, 9223372036854775807],
        ),
        # float / double
        ("SELECT 1.5, 2.25", [1.5, 2.25]),
        # text
        ("SELECT 'hello', 'world'", ["hello", "world"]),
        # json
        ('SELECT {"a": 1, "b": "x"}', {"a": 1, "b": "x"}),
        ("SELECT [1, 2, 3]", [1, 2, 3]),
        # date / timestamp
        (
            "SELECT DATE '2020-01-01', TIMESTAMP '2020-01-01 12:34:56'::at_timezone('UTC')",
            [
                date.fromisoformat("2020-01-01"),
                datetime.fromisoformat("2020-01-01 07:34:56+00"),
            ],
        ),
        # uuid
        (
            "SELECT UUID '550e8400-e29b-41d4-a716-446655440000'",
            uuid.UUID("550e8400-e29b-41d4-a716-446655440000"),
        ),
        # vector
        ("SELECT VECTOR [1.2, 2.3]", [1.2, 2.3]),
    ],
)
def test_select_literals(db_setup, query, expected_data_value):
    # implicitly happening right now no type code
    # TODO: implement COLUMN and description to test with type codes
    _, cur, _ = db_setup
    cur.execute(query)
    row = cur.fetchone()
    # The cursor returns rows as dicts mapping column names to values (col1, col2, ...)
    if isinstance(expected_data_value, list):
        # Prefer multi-column results (col1, col2, ...). Some queries return a single
        # column where the value itself is a list/array (e.g. SELECT [1,2,3]). Handle both.
        if all(f"col{i+1}" in row for i in range(len(expected_data_value))):
            got = [row[f"col{i+1}"] for i in range(len(expected_data_value))]
            assert got == expected_data_value
        else:
            # Single-column containing the list
            assert row["col1"] == expected_data_value
    else:
        assert row["col1"] == expected_data_value
