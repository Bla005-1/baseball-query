from .errors import EmptyQueryError

class SQLQuery:
    def __init__(self):
        self.select = []
        self.from_table = None
        self.where = []
        self.group_by = []
        self.order_by = []

    def add_select(self, column: str):
        if column not in self.select:
            self.select.append(column)

    def set_from_table(self, table: str):
        self.from_table = table

    def add_where(self, condition: str):
        self.where.append(condition)

    def add_group_by(self, column: str):
        if column not in self.group_by:
            self.group_by.append(column)

    def add_order_by(self, column: str):
        self.order_by.append(column)

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


class BaseStrSQLQuery(SQLQuery):
    def __init__(self, base_query: str):
        super().__init__()
        self.base_query = base_query

    def build_query(self) -> str:
        query = self.base_query
        if self.where:
            query += f' WHERE {" AND ".join(self.where)}'
        if self.group_by:
            query += f' GROUP BY {", ".join(self.group_by)}'
        if self.order_by:
            query += f' ORDER BY {", ".join(self.order_by)}'
        return query
