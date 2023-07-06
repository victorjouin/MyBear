import pytest
from lib.mybear.dataframe import Dataframe
from lib.mybear.series import Series


def test_max():
    series1 = Series([1, 2, 3], "A")
    series2 = Series([4, 5, 6], "B")
    series_list = [series1, series2]
    df = Dataframe(series_list)
    max_df = df.max()
    assert max_df.data[0].data == [3]
    assert max_df.data[1].data == [6]

def test_min():
    series1 = Series([1, 2, 3], "A")
    series2 = Series([4, 5, 6], "B")
    series_list = [series1, series2]
    df = Dataframe(series_list)
    min_df = df.min()
    assert min_df.data[0].data == [1]
    assert min_df.data[1].data == [4]


def test_mean():
    series1 = Series([1, 2, 3], "A")
    series2 = Series([4, 5, 6], "B")
    series_list = [series1, series2]
    df = Dataframe(series_list)
    mean_df = df.mean()
    assert mean_df.data[0].data == [2]
    assert mean_df.data[1].data == [5]


def test_count():
    series1 = Series([1, 2, 3], "A")
    series2 = Series([4, 5, 6], "B")
    series_list = [series1, series2]
    df = Dataframe(series_list)
    count_df = df.count()
    assert count_df.data[0].data == [3]
    assert count_df.data[1].data == [3]


def test_std():
    series1 = Series([1, 2, 3], "A")
    series2 = Series([4, 5, 6], "B")
    series_list = [series1, series2]
    df = Dataframe(series_list)
    std_df = df.std()
    assert std_df.data[0].data == pytest.approx([0.816496580927726])
    assert std_df.data[1].data == pytest.approx([0.816496580927726])


def test_empty_dataframe():
    df = Dataframe([])
    assert df.data == []


def test_iloc():
    series1 = Series([1, 2, 3], "A")
    series2 = Series([4, 5, 6], "B")
    series_list = [series1, series2]
    df = Dataframe(series_list)
    assert df.iloc(1, 1) == 5
    result_df = df.iloc(0, slice(0, 2))
    assert isinstance(result_df, Dataframe)
    assert len(result_df.data) == 2
    assert result_df.data[0].name == "A"
    assert result_df.data[0].data == [1]
    assert result_df.data[1].name == "B"
    assert result_df.data[1].data == [4]
    result_series = df.iloc(slice(0, 2), 0)
    assert isinstance(result_series, Series)
    assert result_series.name == "A"
    assert result_series.data == [1, 2]
    result_df = df.iloc(slice(0, 2), slice(0, 2))
    assert isinstance(result_df, Dataframe)
    assert len(result_df.data) == 2
    assert result_df.data[0].name == "A"
    assert result_df.data[0].data == [1, 2]
    assert result_df.data[1].name == "B"
    assert result_df.data[1].data == [4, 5]

def test_dataframe_equality():
    series1 = Series([1, 2, 3], "A")
    series2 = Series([4, 5, 6], "B")
    df1 = Dataframe([series1, series2])
    df2 = Dataframe([series1, series2])
    assert df1 == df2
    df3 = Dataframe([series1])
    assert df1 != df3


def test_read_csv():
    df = Dataframe.read_csv("./tests/data.csv")
    column_names = [series.name for series in df.data]
    assert column_names == ["Name", "Age", "Occupation"]
    assert df.data[0].data == ["Alice", "Bob", "Charlie"]
    assert df.data[1].data == ["30", "25", "35"]
    assert df.data[2].data == ["Doctor", "Engineer", "Teacher"]


def test_read_json():
    df = Dataframe.read_json("./tests/data_col.json", orient="columns")
    df1 = Dataframe.read_json("./tests/data_records.json", orient="records")
    column_names = [series.name for series in df.data]
    column_names1 = [series.name for series in df1.data]
    assert len(df.data) == 3
    assert len(df1.data) == 3
    assert column_names == ["Name", "Age", "Occupation"]
    assert column_names1 == ["Name", "Age", "Occupation"]
    assert df.data[0].data == ["Alice", "Bob", "Charlie"]
    assert df1.data[0].data == ["Alice", "Bob", "Charlie"]
    assert df.data[1].data == [30, 25, 35]
    assert df1.data[1].data == [30, 25, 35]
    assert df.data[2].data == ["Doctor", "Engineer", "Teacher"]
    assert df1.data[2].data == ["Doctor", "Engineer", "Teacher"]

def test_groupby():
    series1 = Series([1, 2, 3, 4, 5, 1, 1, 1, 2, 3], "A")
    series2 = Series([10, 20, 30, 40, 50, 60, 70, 80, 90, 100], "B")
    series3 = Series([100, 200, 300, 400, 500, 600, 700, 800, 900, 1000], "C")
    series_list = [series1, series2, series3]
    df = Dataframe(series_list)

    grouped_df = df.groupby(
        by=["A"],
        agg={
            "B": min,
            "C": max
        }
    )

    assert grouped_df.data[0].data == [1, 2, 3, 4, 5]
    assert grouped_df.data[1].data == [10, 20, 30, 40, 50]
    assert grouped_df.data[2].data == [800, 900, 1000, 400, 500]