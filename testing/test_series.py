import pytest
from lib.mybear.series import Series


def test_series_max():
    series = Series([1, 2, 3, 4, 5], "Test max")
    assert series.max() == 5


def test_series_min():
    series = Series([1, 2, 3, 4, 5], "Test min")
    assert series.min() == 1


def test_series_mean():
    series = Series([1, 2, 3, 4, 5], "Test mean")
    assert series.mean() == 3.0


def test_series_std():
    series = Series([1, 2, 3, 4, 5], "Test std")
    assert series.std() == pytest.approx(1.4142135623730951)


def test_series_count():
    series = Series([1, 2, 3, None, 5], "Test count")
    assert series.count() == 4


def test_series_iloc():
    series = Series([1, 2, 3, 4, 5], "Test iloc")
    assert series.iloc(1) == 2
    assert series.iloc(slice(1, 3)) == [2, 3]


def test_series_eq():
    series1 = Series([1, 2, 3, 4, 5], "Test eq")
    series2 = Series([1, 2, 3, 4, 5], "Test eq")
    series3 = Series([1, 2, 3], "Test eq")
    assert series1 == series2
    assert not (series1 == series3)


def test_series_repr():
    series = Series([1, 2, 3, 4, 5], "Test repr")
    assert repr(series) == "Series([1, 2, 3, 4, 5], name=Test repr)"
