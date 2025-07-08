from .errors import EmptyQueryError
from typing import Self

class SQLQuery:
    def __init__(self):
        self.select = []
        self.from_table = None
        self.where = []
        self.group_by = []
        self.order_by = []

    def add_select(self, column: str) -> Self:
        if column not in self.select:
            self.select.append(column)
        return self

    def set_from_table(self, table: str) -> Self:
        self.from_table = table
        return self

    def add_where(self, condition: str) -> Self:
        self.where.append(condition)
        return self

    def add_group_by(self, column: str) -> Self:
        if column not in self.group_by:
            self.group_by.append(column)
        return self

    def add_order_by(self, column: str) -> Self:
        self.order_by.append(column)
        return self

    def build_query(self) -> str:
        if not self.from_table:
            raise EmptyQueryError('FROM clause is missing.')
        if len(self.select) == 0:
            raise EmptyQueryError('SELECT clause is missing.')
        query = f'SELECT {", ".join(self.select)} FROM {self.from_table}'
        if self.where:
            query += f' WHERE {" AND ".join(self.where)}'
        if self.group_by:
            query += f' GROUP BY {", ".join(self.group_by)}'
        if self.order_by:
            query += f' ORDER BY {", ".join(self.order_by)}'
        return query

    def __str__(self):
        return self.build_query()

    def copy(self) -> 'SQLQuery':
        new_query = SQLQuery()
        new_query.select = self.select[:]  # shallow copy of list
        new_query.from_table = self.from_table  # str, immutable
        new_query.where = self.where[:]  # shallow copy
        new_query.group_by = self.group_by[:]  # shallow copy
        new_query.order_by = self.order_by[:]  # shallow copy
        return new_query


class BaseStrSQLQuery(SQLQuery):
    def __init__(self, base_query: str):
        super().__init__()
        self.base_query = base_query
        select_part, sep, from_part = base_query.rpartition(' FROM ')
        if not sep:
            raise ValueError('Invalid base query: missing FROM clause')
        self.from_table = from_part.strip()
        columns = []
        current = []
        parens = 0
        for char in select_part.replace('SELECT', '', 1):
            if char == ',' and parens == 0:
                columns.append(''.join(current).strip())
                current = []
            else:
                current.append(char)
                if char == '(':
                    parens += 1
                elif char == ')':
                    parens = max(parens - 1, 0)
        if current:
            columns.append(''.join(current).strip())
        self.select = columns

    def build_query(self) -> str:
        query = self.base_query
        if self.where:
            query += f' WHERE {" AND ".join(self.where)}'
        if self.group_by:
            query += f' GROUP BY {", ".join(self.group_by)}'
        if self.order_by:
            query += f' ORDER BY {", ".join(self.order_by)}'
        return query
