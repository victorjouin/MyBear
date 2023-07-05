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
        if isinstance(self.data, list):
            return f"Series({self.data}, {self.name})"

    # Get the value of the series

    def iloc(self, i) -> Any:
        # Explication de la fonction iloc
        # Si i est un entier, on retourne la valeur de la liste à l'index i
        # Si i est une liste, on retourne une liste de valeur de la liste à l'index i
        # Si i est un slice, on retourne une liste de valeur de la liste entre les index i.start et i.stop
        if isinstance(i, slice):
            return Series(self.data[i.start:i.stop])
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
