import csv
import json
from typing import Any, List, Dict, Callable, Union
from .series import Series
import numpy as np


class DataFrame:
    def __init__(self, series_list: List[Series]):
        self.data = series_list
        self.columns = [s.name for s in series_list]

    def max(self):
        return DataFrame([Series([max(s.data)], s.name) for s in self.data])

    def min(self):
        return DataFrame([Series([min(s.data)], s.name) for s in self.data])

    def mean(self):
        return DataFrame([Series([np.mean(s.data)], s.name) for s in self.data])

    def count(self):
        return DataFrame([Series([len(s.data)], s.name) for s in self.data])

    def std(self):
        return DataFrame([Series([np.std(s.data)], s.name) for s in self.data])

    def iloc(self, i, j):
        if isinstance(i, slice) and isinstance(j, slice):
            return DataFrame([Series(s.data[i], s.name) for s in self.data[j]])
        elif isinstance(i, slice):
            return Series(self.data[j].data[i], self.data[j].name)
        elif isinstance(j, slice):
            return DataFrame(
                [Series([self.data[s].data[i]], self.data[s].name) for s in range(*j.indices(len(self.data)))])
        else:
            return self.data[j].data[i]

    def __str__(self):
        return "\n".join(f"{s.name}: {s.data}" for s in self.data)

    @staticmethod
    def read_csv(path: str, delimiter: str = ",") -> 'DataFrame':
        def try_convert_to_number(s):
            try:
                return int(s)
            except ValueError:
                try:
                    return float(s)
                except ValueError:
                    return s

        with open(path, 'r') as f:
            reader = csv.reader(f, delimiter=delimiter)
            data = list(reader)
            series = [Series([try_convert_to_number(row[i]) for row in data[1:]], data[0][i])
                      for i in range(len(data[0]))]
            return DataFrame(series)

    @staticmethod
    def read_json(path: str, orient: str = "records") -> 'DataFrame':
        with open(path, 'r') as f:
            data = json.load(f)
            if orient == "records":
                keys = data[0].keys()
                series = [Series([d[k] for d in data], k) for k in keys]
            elif orient == "columns":
                series = [Series(v, k) for k, v in data.items()]
            else:
                raise ValueError("Invalid value for argument 'orient'.")
            return DataFrame(series)

    def groupby(self, by: List[str] | str, agg: Dict[str, Callable[[List[Any]], Any]]) -> 'DataFrame':
        if isinstance(by, str):
            by = [by]
        by_indices = [self.columns.index(b) for b in by]
        groups = {}
        for row in zip(*[s.data for s in self.data]):
            key = tuple(row[i] for i in by_indices)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)
        new_rows = []
        for key, rows in groups.items():
            new_row = list(key)
            for col, func in agg.items():
                col_index = self.columns.index(col)
                new_row.append(func([row[col_index] for row in rows]))
            new_rows.append(new_row)
        new_columns = list(by) + list(agg.keys())
        return DataFrame([Series([row[i] for row in new_rows], name) for i, name in enumerate(new_columns)])

    def __eq__(self, other):
        if not isinstance(other, DataFrame):
            return False
        if len(self.data) != len(other.data):
            return False
        for s1, s2 in zip(self.data, other.data):
            if s1.name != s2.name or s1.data != s2.data:
                return False
        return True

    def join(
            self,
            other: 'DataFrame',
            left_on: Union[List[str], str],
            right_on: Union[List[str], str],
            how: str = "left"
    ) -> 'DataFrame':

        if isinstance(left_on, str):
            left_on = [left_on]
        if isinstance(right_on, str):
            right_on = [right_on]

        left_indices = [self.columns.index(col) for col in left_on]
        right_indices = [other.columns.index(col) for col in right_on]

        joined_rows = []
        for left_row in zip(*[s.data for s in self.data]):
            left_key = tuple(left_row[i] for i in left_indices)
            for right_row in zip(*[s.data for s in other.data]):
                right_key = tuple(right_row[i] for i in right_indices)
                if left_key == right_key:
                    # Exclude the join columns from the right dataframe no Double columns allowed
                    joined_rows.append(
                        left_row + tuple(value for i, value in enumerate(right_row) if i not in right_indices))
                    break
            else:
                if how in {"left", "outer"}:
                    joined_rows.append(left_row + tuple(None for _ in other.data))

        if how in {"right", "outer"}:
            for right_row in zip(*[s.data for s in other.data]):
                right_key = tuple(right_row[i] for i in right_indices)
                if not any(right_key == tuple(left_row[i] for i in left_indices) for left_row in
                           zip(*[s.data for s in self.data])):
                    joined_rows.append(tuple(None for _ in self.data) + right_row)

        # No Double columns allowed
        other_columns = [col for col in other.columns if col not in right_on]
        joined_columns = self.columns + other_columns

        joined_data = list(zip(*joined_rows))
        joined_data = [list(col_data) for col_data in zip(*joined_rows)]
        return DataFrame([Series(col_data, col_name) for col_data, col_name in zip(joined_data, joined_columns)])

