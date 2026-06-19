"""Unit tests for the Polars processing step (etl.transform.process)."""
import polars as pl

from etl.transform import process, _norm


def test_snake_case_columns():
    df = process(pl.DataFrame({"Summons Number": ["1"], "Plate ID!": ["A"]}))
    assert df.columns == ["summons_number", "plate_id"]


def test_norm_helper():
    assert _norm("Owner's House #") == "owner_s_house"
    assert _norm("  ") == "col"


def test_numeric_inference():
    df = process(pl.DataFrame({"n": ["1", "2", "3"], "x": ["1.5", "2.0", "3.5"]}))
    assert df["n"].dtype == pl.Int64
    assert df["x"].dtype == pl.Float64


def test_leading_zero_stays_text():
    # zip codes / codes must not be coerced to int (would drop the zero)
    df = process(pl.DataFrame({"zip": ["00123", "07008", "10001"]}))
    assert df["zip"].dtype == pl.Utf8
    assert "00123" in df["zip"].to_list()


def test_mixed_column_stays_text():
    df = process(pl.DataFrame({"c": ["1", "2", "abc"]}))
    assert df["c"].dtype == pl.Utf8


def test_trim_and_empty_to_null():
    df = process(pl.DataFrame({"c": ["  a  ", ""]}))
    vals = df["c"].to_list()
    assert "a" in vals
    assert None in vals


def test_dedup_exact_rows():
    df = process(pl.DataFrame({"a": ["1", "1", "2"], "b": ["x", "x", "y"]}))
    assert df.height == 2


def test_empty_frame():
    df = process(pl.DataFrame())
    assert df.is_empty()
