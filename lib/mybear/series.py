from typing import Any


class Series:
    def __init__(self, data: list, name: str):
        self.size = len(data)
        self.data = data
        self.name = name

    # Display the series
    def __str__(self, other):
        if isinstance(other, Series):
            return self.data == other.data
        return False

    # Display information about the series
    def __repr__(self):
        return f"Series({self.data}, name={self.name})"

    # Get the value of the series

    def iloc(self, i) -> Any:
        if isinstance(i, slice):
            return list(self.data[i.start:i.stop])
        return self.data[i]

    def max(self):
        return max(self.data)

    def min(self):
        return min(self.data)

    def mean(self):
        return sum(self.data) / self.size

    # cart type bref l'homogénéité des données
    def std(self):
        mean = self.mean()
        var = 0
        for i in self.data:
            var += (i - mean) ** 2
        return (var / self.size) ** 0.5

    def count(self):
        count = 0
        for i in self.data:
            if i is not None:
                count += 1
        return count

    def __eq__(self, other):
        if not isinstance(other, Series):
            return False
        if self.name != other.name or self.data != other.data:
            return False
        return True



