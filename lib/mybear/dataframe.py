import csv
import json
from typing import Any, List, Dict, Callable
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

    @staticmethod
    def read_csv(path: str, delimiter: str = ",") -> 'DataFrame':
        with open(path, 'r') as f:
            reader = csv.reader(f, delimiter=delimiter)
            data = list(reader)
            series = [Series([row[i] for row in data[1:]], data[0][i]) for i in range(len(data[0]))]
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
        return DataFrame([Series([row[i] for row in new_rows], name) for i, name in enumerate(self.columns)])

    def __eq__(self, other):
        if not isinstance(other, DataFrame):
            return False
        if len(self.data) != len(other.data):
            return False
        for s1, s2 in zip(self.data, other.data):
            if s1.name != s2.name or s1.data != s2.data:
                return False
        return True
