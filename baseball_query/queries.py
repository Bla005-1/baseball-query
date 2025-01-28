from typing import List, Tuple, Dict
from .errors import EmptyQueryError
from .metric_manager import DBMetric


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


class QueryBuilder:
    def __init__(self, sql_query: SQLQuery = None):
        self.metrics_dict = {}
        self.sql_query = sql_query or SQLQuery()
        self.empty = False
        self.args = []
        self.name_column = 'name'
        self.team_column = 'team_name'

    def add_name(self, name_values: str | List[str]):
        self.add_dynamic_where(self.name_column, name_values)

    def add_team(self, team_values: str | List[str]):
        self.add_dynamic_where(self.team_column, team_values)

    def add_dates(self, dates: Tuple[str, str]):
        if dates is not None:
            if len(dates) == 2:
                if dates[0] and dates[1]:
                    self.add_raw_where('official_date BETWEEN %s AND %s', [dates[0], dates[1]])

    def add_year(self, year: str):
        self.add_dynamic_where('season', year)

    def add_raw_where(self, where_clause: str, args: List[str] | str = None):
        self.sql_query.add_where(where_clause)
        if isinstance(args, str):
            self.args.append(args)
        elif args:
            self.args.extend(args)

    def order_by(self, column: str):
        self.sql_query.add_order_by(column)

    def add_group_column(self, column: str | List[str]):
        if isinstance(column, str):
            self.sql_query.add_group_by(column)
        else:
            for c in column:
                self.sql_query.add_group_by(c)

    def add_dynamic_where(self, column: str, values: str | List[str]):
        if not values:
            self.add_group_column(column)
            return
        if isinstance(values, list):
            if len(values) == 1:
                self.sql_query.add_where(f'{column} = %s')
            else:
                self.sql_query.add_where(f'{column} IN ({", ".join("%s" * len(values))})')
            self.args.extend(values)
        else:
            self.sql_query.add_where(f'{column} = %s')
            self.args.append(values)

    def get_query(self) -> str:
        try:
            return self.sql_query.build_query()
        except EmptyQueryError:
            self.empty = True
            return 'empty query'

    def get_args(self) -> List[str]:
        return self.args

    def process_metrics(self, metric_keys: List[str], metric_sqls: List[str]):
        for m, db_metric in self.metrics_dict.items():
            if m not in metric_keys:
                continue
            if not db_metric.sql_value:
                continue
            if db_metric.sql_value not in metric_sqls:
                metric_sqls.append(db_metric.sql_value)
                self.sql_query.add_select(db_metric.sql_value)

    def __str__(self):
        return self.get_query()

    def __bool__(self):
        self.get_query()
        return not self.empty


class TotalsBuilder(QueryBuilder):
    def __init__(self, metrics_dict: Dict[str, DBMetric], metric_keys: List[str], player_type: str):
        super().__init__()
        self.metrics_dict = metrics_dict
        self.metric_sql = []
        self.metric_keys = metric_keys
        self.name_column = 'name'
        self.team_column = 'team_name'
        self.process_metrics(self.metric_keys, self.metric_sql)
        self.sql_query.set_from_table('hitters' if player_type == 'batter' else 'pitchers')


class PlaysBuilder(QueryBuilder):
    def __init__(self, metrics_dict: Dict[str, DBMetric], metric_keys: List[str], player_type: str):
        super().__init__()
        self.metrics_dict = metrics_dict
        self.metric_keys = metric_keys
        self.name_column = player_type + '_name'
        self.team_column = 'team_batting' if player_type == 'batter' else 'team_fielding'
        self.metric_sql = []
        for key, replacement in [('team_name', self.team_column), ('name', self.name_column)]:
            try:
                index = self.metric_keys.index(key)
                self.metric_keys[index] = replacement
            except ValueError:
                continue
        self.process_metrics(self.metric_keys, self.metric_sql)
        self.sql_query.set_from_table('all_plays')


class SQLJoinBuilder:
    def __init__(self, query1: QueryBuilder, query2: QueryBuilder, merge_on: List[str], join_type: str = "INNER"):
        self.query1 = query1
        self.query2 = query2
        self.merge_on = merge_on
        self.join_type = join_type.upper()  # Ensure join type is uppercase (e.g., INNER, LEFT, etc.)

    def build_join_query(self) -> str:
        # Ensure both queries have their SELECT statements
        query1_sql = self.query1.sql_query
        query2_sql = self.query2.sql_query

        if not query1_sql.select or not query2_sql.select:
            raise ValueError('Both queries must have SELECT clauses before joining.')

        # Combine SELECT statements
        select_clause = (
            ', '.join([f't1.{col}' for col in query1_sql.select]) +
            ', ' +
            ', '.join([f't2.{col}' for col in query2_sql.select])
        )

        # FROM and JOIN clause
        from_clause = f'{query1_sql.from_table} AS t1'
        join_clause = f'{self.join_type} JOIN {query2_sql.from_table} AS t2'

        # ON clause for merging
        on_conditions = [f't1.{col} = t2.{col}' for col in self.merge_on]
        on_clause = f'ON {" AND ".join(on_conditions)}'

        # WHERE clauses
        where_conditions = list(set(query1_sql.where + query2_sql.where))
        where_clause = f'WHERE {" AND ".join(where_conditions)}' if where_conditions else ''

        # GROUP BY clause
        group_by_clause = (
            f'GROUP BY {", ".join(list(set(query1_sql.group_by + query2_sql.group_by)))}'
            if query1_sql.group_by or query2_sql.group_by
            else ""
        )

        # ORDER BY clause
        order_by_clause = (
            f'ORDER BY {", ".join(list(set(query1_sql.order_by + query2_sql.order_by)))}'
            if query1_sql.order_by or query2_sql.order_by
            else ""
        )

        # Build the full query
        query = (
            f'SELECT {select_clause} '
            f'FROM {from_clause} '
            f'{join_clause} {on_clause} '
            f'{where_clause} '
            f'{group_by_clause} '
            f'{order_by_clause}'
        ).strip()

        return query

    def get_args(self) -> List[str]:
        return list(set(self.query1.get_args() + self.query2.get_args()))