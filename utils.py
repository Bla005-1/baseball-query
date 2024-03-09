import sqlite3


def connect():
    conn = sqlite3.connect('baseball_plays.db')
    cursor = conn.cursor()
    return conn, cursor


class DataRow(dict):
    def __init__(self, data):
        super().__init__(data)
        self.fix_description()

    def fix_description(self):
        if 'In play' in self['description']:
            self['description'] = 'In Play'

    def matches_criteria(self, criteria: dict):
        for key, value in criteria.items():
            if isinstance(value, list):
                if self.get(key) not in value:
                    return False
            else:
                if self.get(key) != value:
                    return False
        return True


class DataRowContainer(list):
    def __init__(self, rows):
        super().__init__(rows)

    def sort_by(self, key):
        data = {}
        for r in self:
            key_value = r.get(key)
            if key_value is not None:
                if key_value not in data:
                    data[key_value] = DataRowContainer([])
                data[key_value].append(r)
        return data

    def matches_criteria(self, criteria):
        return DataRowContainer([r for r in self if r.matches_criteria(criteria)])

    def get(self, key):
        return [r.get(key) for r in self if r.get(key) is not None]
