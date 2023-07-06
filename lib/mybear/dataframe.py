import csv
import json

from .series import Series


class Dataframe(Series):
    # TODO: Implement the following methods:
    #  - min
    #  - max
    #  - mean
    #  - std
    #  - count
    #  - iloc

    @staticmethod
    def __init__(self, *args, name='Dataframe'):
        data = []
        if len(args) == 1:
            if isinstance(args[0], list) and all(isinstance(element, Series) for element in args[0]):
                for serie in args[0]:
                    data.append(serie)

                self.name = name
                self.data = data

        elif len(args) == 2 and isinstance(args[0], list) and isinstance(args[1], list):
            nb_columns = len(args[0])
            col_name = args[0]
            col_values = args[1]

            for i in range(nb_columns):
                li = [col_values[i]]
                new_serie = Series(li, col_name[i])
                data.append(new_serie)

            self.name = name
            self.data = data

        else:
            raise ValueError("Invalid arguments for Dataframe constructor")

    def __eq__(self, other):
        """
        surcharge de l'opérateur == pour comparer deux dataframes
        """
        if isinstance(other, Dataframe):

            return self.data == other.data and self.name == other.name
        else:
            return False

    def iloc(self, rows, cols):
        if isinstance(rows, int) and isinstance(cols, int):
            return self.data[cols].iloc(rows)

        elif isinstance(rows, int) and isinstance(cols, slice):
            all_rows = []
            all_columns = []
            start = cols.start
            stop = cols.stop
            for i in range(start, stop):
                all_columns.append(self.data[i].name)
                all_rows.append(self.data[i].data[rows])
            return Dataframe(all_columns, all_rows)

        elif isinstance(rows, slice) and isinstance(cols, int):
            data = []
            column = self.data[cols]
            start = rows.start
            stop = rows.stop
            for i in range(start, stop):
                data.append(column.data[i])

            return Series(data, self.data[cols].name)

        elif isinstance(rows, slice) and isinstance(cols, slice):
            c_start = cols.start
            c_stop = cols.stop

            series_list = []
            for i in range(c_start, c_stop):
                series_list.append(self.iloc(rows, i))

            return Dataframe(series_list)

        else:
            raise ValueError("Invalid arguments for iloc")

    def max(self) -> 'Dataframe':
        """
        Get the max value of each column of the dataframe
        """
        column_list = self.data
        data = []

        for column in column_list:
            name = column.name
            val = [column.max()]
            data.append(Series(val, name))

        return Dataframe(data, name='MaxValues')

    def min(self):
        """
        Get the min value of each column of the dataframe
        """
        column_list = self.data
        data = []

        for column in column_list:
            name = column.name
            val = [column.min()]
            data.append(Series(val, name))

        return Dataframe(data, name='MinValues')

    def mean(self):
        """
        Get the mean value of each column of the dataframe
        """
        column_list = self.data
        data = []

        for column in column_list:
            name = column.name
            val = [column.mean()]
            data.append(Series(val, name))

        return Dataframe(data, name='MeanValues')

    def std(self):
        """
        print the standard deviation of each column of the dataframe
        """
        column_list = self.data
        data = []

        for column in column_list:
            name = column.name
            val = [column.std()]
            data.append(Series(val, name))

        return Dataframe(data, name='StandardDeviationValues')

    def count(self):
        column_list = self.data
        data = []

        for column in column_list:
            name = column.name
            val = [column.count()]
            data.append(Series(val, name))

        return Dataframe(data, name='NbOfValues')

    def __repr__(self):
        """
       overloading of the repr function to print a dataframe
        """
        repr_str = f"{self.name}:\n"
        for series in self.data:
            repr_str += f"{series.name}: {series.data}\n"
        return repr_str

    from typing import List, Dict, Callable, Any

    def groupby(self, by: List[str] | str, agg: Dict[str, Callable[[List[Any]], Any]]) -> 'Dataframe':
        """
        regroup the dataframe by the cols given in the by parameter and aggregate the other columns
        reminds me of group by from MongoDB
        """
        if isinstance(by, str):
            by = [by]

        grouped_data = {}
        group_indices = {}
        for i, row in enumerate(self.data[0].data):
            key = tuple(self.data[j].data[i] for j in range(len(self.data)) if self.data[j].name in by)
            if key not in grouped_data:
                grouped_data[key] = [[] for _ in self.data]
                group_indices[key] = []

            for j, column in enumerate(self.data):
                grouped_data[key][j].append(column.data[i])

            group_indices[key].append(i)
        aggregated_data = [[] for _ in self.data]
        for key, indices in group_indices.items():
            for j, column in enumerate(self.data):
                if column.name in agg:
                    aggregated_value = agg[column.name]([column.data[i] for i in indices])
                else:
                    aggregated_value = column.data[indices[0]]
                aggregated_data[j].append(aggregated_value)
        new_series = []
        for j, column in enumerate(self.data):
            new_name = column.name
            new_data = aggregated_data[j]
            new_series.append(Series(new_data, new_name))

        return Dataframe(new_series)

    def read_csv(path: str, delimiter: str = ",") -> 'Dataframe':
        """
        Read a CSV file and return a Dataframe
        """
        try:
            with open(path, "r") as file:
                r = csv.reader(file, delimiter=delimiter)
                headers = next(r)
                columns = [[] for _ in headers]
                for row in r:
                    if len(row) != len(headers):
                        raise IndexError("Malformed CSV file")
                    for i, cell in enumerate(row):
                        columns[i].append(cell)
                series = [Series(col, name) for col, name in zip(columns, headers)]
                return Dataframe(series)
        except IndexError as e:
            print(f"Error while reading the CSV file: {e}")
            raise

    @staticmethod
    def read_json(path: str, orient: str = "records") -> 'Dataframe':
        """
        Same as read_csv but for JSON files.
        """
        try:
            with open(path, "r") as file:
                data = json.load(file)

                if orient == "records":
                    headers = data[0].keys()
                    columns = {header: [] for header in headers}

                    for record in data:
                        if len(record) != len(headers):
                            raise IndexError("Row has different number of columns than the headers.")

                        for key, value in record.items():
                            columns[key].append(value)

                    series = [Series(col, name) for name, col in columns.items()]

                elif orient == "columns":
                    series = [Series(values, name) for name, values in data.items()]

                else:
                    raise ValueError(f"Unsupported JSON orientation: {orient}")

                return Dataframe(series)

        except IndexError as e:
            print(f"Error reading JSON file: {e}")
            raise
