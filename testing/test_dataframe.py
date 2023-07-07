import pytest
from lib.mybear.dataframe import DataFrame as Dataframe
from lib.mybear.series import Series


def test_max():
    series1 = Series([10, 12, 33], "A")
    series2 = Series([4, 52, 63], "B")
    series_list = [series1, series2]
    df = Dataframe(series_list)
    max_df = df.max()
    assert max_df.data[0].data == [33]
    assert max_df.data[1].data == [63]


def test_min():
    series1 = Series([10, 12, 33], "A")
    series2 = Series([4, 52, 63], "B")
    series_list = [series1, series2]
    df = Dataframe(series_list)
    min_df = df.min()
    assert min_df.data[0].data == [10]
    assert min_df.data[1].data == [4]


def test_mean():
    series1 = Series([1, 2, 4, 3], "A")
    series2 = Series([4, 5, 6, 7], "B")
    series_list = [series1, series2]
    df = Dataframe(series_list)
    mean_df = df.mean()
    assert mean_df.data[0].data == [2.5]
    assert mean_df.data[1].data == [5.5]


def test_count():
    series1 = Series([1, 2, 3, 4, 5], "A")
    series2 = Series([4, 5, 6, 7, 8, 9], "B")
    series_list = [series1, series2]
    df = Dataframe(series_list)
    count_df = df.count()
    assert count_df.data[0].data == [5]
    assert count_df.data[1].data == [6]


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
    df = Dataframe.read_csv("../data/data.csv")
    column_names = df.columns
    assert column_names == ["Age", "Name", "Country", "Job", "Salary"]
    assert df.data[0].data == [25, 30, 35, 40, 45]
    assert df.data[1].data == ["John", "Emma", "Sophie", "Mike", "David"]
    assert df.data[2].data == ["USA", "Canada", "Australia", "USA", "UK"]
    assert df.data[3].data == ["Engineer", "Doctor", "Teacher", "Lawyer", "Engineer"]
    assert df.data[4].data == [70000, 80000, 60000, 90000, 85000]


def test_read_json():
    df = Dataframe.read_json("../data/data_columns.json", orient="columns")
    df1 = Dataframe.read_json("../data/data.json", orient="records")
    column_names = df.columns
    column_names1 = df1.columns
    assert len(df.data) == 5
    assert len(df1.data) == 5
    assert column_names == ["Age", "Name", "Country", "Job", "Salary"]
    assert column_names1 == ["Age", "Name", "Country", "Job", "Salary"]
    assert df.data[0].data == [25, 30, 35, 40, 45]
    assert df1.data[0].data == [25, 30, 35, 40, 45]
    assert df.data[1].data == ["John", "Emma", "Sophie", "Mike", "David"]
    assert df1.data[1].data == ["John", "Emma", "Sophie", "Mike", "David"]
    assert df.data[2].data == ["USA", "Canada", "Australia", "USA", "UK"]
    assert df1.data[2].data == ["USA", "Canada", "Australia", "USA", "UK"]



def test_groupby():
    series_gender = Series(["M", "F", "M", "M", "F", "F"], "Gender")
    series_department = Series(["Sales", "HR", "IT", "Sales", "IT", "HR"], "Department")
    series_salary = Series([5000, 6000, 7000, 5500, 6500, 6200], "Salary")
    df = Dataframe([series_gender, series_department, series_salary])

    grouped_df = df.groupby("Department", {"Salary": sum})

    # Les assertions dépendent de ce que fait exactement votre méthode groupby.
    # Par exemple, si groupby calcule la somme des salaires par département, vous pourriez avoir :
    assert grouped_df.data[0].data == ["Sales", "HR", "IT"]
    assert grouped_df.data[1].data == [10500, 12200, 13500]




def test_join():
    df1 = Dataframe([
        Series([1, 2, 3], 'id'),
        Series(['Alice', 'Bob', 'Charlie'], 'name')
    ])
    df2 = Dataframe([
        Series([2, 3, 4], 'id'),
        Series(['English', 'Math', 'Science'], 'subject')
    ])

    joined = df1.join(df2, left_on='id', right_on='id')

    expected = Dataframe([
        Series([1, 2, 3], 'id'),
        Series(['Alice', 'Bob', 'Charlie'], 'name'),
        Series([None, 'English', 'Math'], 'subject')
    ])

    assert joined == expected
    assert joined == expected
