class DataFrame:
    def __init__(self, data=None, columns=None):
        self.data = list(data or [])
        self.columns = columns or (list(self.data[0].keys()) if self.data else [])

    def to_dict(self, orient='records'):
        return list(self.data)

    def __len__(self):
        return len(self.data)

    def __getitem__(self, item):
        return [row[item] for row in self.data]

    class _ILoc:
        def __init__(self, outer):
            self.outer = outer

        def __getitem__(self, idx):
            return self.outer.data[idx]

    @property
    def iloc(self):
        return self._ILoc(self)


class Series(dict):
    pass


def merge(df1, df2, on=None, how='inner'):
    if on is None:
        return DataFrame(df1.data + df2.data)
    merged = []
    for r1 in df1.data:
        for r2 in df2.data:
            if all(r1[k] == r2[k] for k in on):
                merged.append({**r1, **r2})
    return DataFrame(merged)
