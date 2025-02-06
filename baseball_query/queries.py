from typing import List, Tuple, Self
from .errors import EmptyQueryError
from .metric_manager import DBMetric
from abc import ABC, abstractmethod
from .sql_query import SQLQuery

class BaseQueryBuilder(ABC):
    def __init__(self, player_type):
        self.player_type = player_type
        self.empty = False
        self.metric_names: List[str] = []
        self.group_columns: List[str] = []

    @abstractmethod
    def get_query(self) -> str:
        pass

    @abstractmethod
    def get_args(self) -> List[str]:
        pass

    @abstractmethod
    def add_select(self, metric: DBMetric) -> Self:
        pass

    @abstractmethod
    def add_group_by(self, column: str | List[str]) -> Self:
        pass

    @staticmethod
    def _parse_select(expression: str) -> Tuple:
        if ' AS ' in expression:
            clauses = expression.split(' AS ')
            alias = clauses[1].strip()
        else:
            alias = expression
        return expression, alias

    def get_group_columns(self) -> List[str]:
        return self.group_columns

    def get_metric_names(self) -> List[str]:
        return self.metric_names

    def __str__(self):
        return self.get_query()

    def __bool__(self):
        self.get_query()
        return not self.empty


class SingleQueryBuilder(BaseQueryBuilder):
    def __init__(self, player_type: str = '', sql_query: SQLQuery = None):
        super().__init__(player_type)
        self.sql_query = sql_query or SQLQuery()
        self.args = []
        self.name_column = 'name'
        self.team_column = 'team_name'

    def add_name(self, name_values: str | List[str]) -> Self:
        self.add_dynamic_where(self.name_column, name_values)
        return self

    def add_team(self, team_values: str | List[str]) -> Self:
        self.add_dynamic_where(self.team_column, team_values)
        return self

    def add_dates(self, dates: Tuple[str, str]):
        if dates is not None:
            if len(dates) == 2:
                if dates[0] and dates[1]:
                    self.add_raw_where('official_date BETWEEN %s AND %s', [dates[0], dates[1]])
        return self

    def add_year(self, year: str) -> Self:
        self.add_dynamic_where('season', year)
        return self

    def add_raw_where(self, where_clause: str, args: List[str] | str = None) -> Self:
        self.sql_query.add_where(where_clause)
        if isinstance(args, str):
            self.args.append(args)
        elif args:
            self.args.extend(args)
        return self

    def order_by(self, column: str) -> Self:
        self.sql_query.add_order_by(column)
        return self

    def add_group_by(self, column: str | List[str]) -> Self:
        if isinstance(column, str):
            self.sql_query.add_group_by(column)
            self.group_columns.append(column)
        else:
            for c in column:
                self.sql_query.add_group_by(c)
                self.group_columns.append(c)
        return self

    def add_dynamic_where(self, column: str, values: str | List[str]) -> Self:
        if not values:
            self.add_group_by(column)
            return self
        if isinstance(values, list):
            if len(values) == 1:
                self.sql_query.add_where(f'{column} = %s')
            else:
                self.sql_query.add_where(f'{column} IN ({", ".join("%s" * len(values))})')
            self.args.extend(values)
        else:
            self.sql_query.add_where(f'{column} = %s')
            self.args.append(values)
        return self

    def get_query(self) -> str:
        try:
            return self.sql_query.build_query()
        except EmptyQueryError:
            self.empty = True
            return 'empty query'

    def get_args(self) -> List[str]:
        return self.args

    def add_select(self, metric: DBMetric) -> Self:
        def update_selects(e: str):
            e, alias = self._parse_select(e)
            self.sql_query.add_select(e)
            self.metric_names.append(alias)
        expression = metric.sql_value
        if '|' in expression:  # might need to change to ! or ~
            values = expression.split('|')
            for v in values:
                update_selects(v)
        elif expression:
            update_selects(expression)
        return self

    def __str__(self):
        return self.get_query()

    def __bool__(self):
        self.get_query()
        return not self.empty


class TotalsBuilder(SingleQueryBuilder):
    def __init__(self, player_type: str):
        super().__init__(player_type)
        self.name_column = 'name'
        self.team_column = 'team_name'
        self.sql_query.set_from_table('hitters' if player_type == 'batter' else 'pitchers')


class PlaysBuilder(SingleQueryBuilder):
    def __init__(self, player_type: str):
        super().__init__(player_type)
        self.name_column = player_type + '_name'
        self.team_column = 'team_batting' if player_type == 'batter' else 'team_fielding'
        self.sql_query.set_from_table('all_plays')

    def add_select(self, metric: DBMetric) -> Self:
        if not metric.is_all_plays:
            return self
        def update_selects(e: str):
            e, alias = self._parse_select(e)
            self.sql_query.add_select(e)
            self.metric_names.append(alias)
        expression = metric.sql_value
        if '|' in expression:  # might need to change to ! or ~
            values = expression.split('|')
            for v in values:
                update_selects(v)
        elif expression:
            update_selects(expression)
        return self

class SQLJoinBuilder:
    def __init__(self, query1: SingleQueryBuilder, query2: SingleQueryBuilder, merge_on: List[str], join_type: str = 'INNER'):
        """
        :param query1: First query builder.
        :param query2: Second query builder.
        :param merge_on: List of tuples specifying column mappings between the two tables (e.g., [('name', 'batter_name'), ('player_id', 'batter_id')]).
        :param join_type: Type of SQL join (e.g., INNER, LEFT).
        """
        if isinstance(query1, PlaysBuilder):
            self.query1 = query1
            self.query2 = query2
        else:
            self.query1 = query2
            self.query2 = query1
        self.merge_on = merge_on
        self.join_type = join_type.upper()  # Ensure join type is uppercase (e.g., INNER, LEFT, etc.)

    def build_join_query(self) -> str:
        if self.query1 and not self.query2:
            return self.query1.get_query()
        elif self.query2 and not self.query1:
            return self.query2.get_query()

        query1_sql = self.query1.sql_query
        query2_sql = self.query2.sql_query

        if not query1_sql.select or not query2_sql.select:
            raise ValueError('Both queries must have SELECT clauses before joining.')

        # Pre-aggregate Query1
        pre_aggregated_query1 = (
            f"(SELECT {', '.join(query1_sql.select)} "
            f"FROM {query1_sql.from_table} "
            f"{'WHERE ' + ' AND '.join(query1_sql.where) if query1_sql.where else ''} "
            f"GROUP BY {', '.join(query1_sql.group_by)}) AS t1"
        )

        # Pre-aggregate Query2
        pre_aggregated_query2 = (
            f"(SELECT {', '.join(query2_sql.select)} "
            f"FROM {query2_sql.from_table} "
            f"{'WHERE ' + ' AND '.join(query2_sql.where) if query2_sql.where else ''} "
            f"GROUP BY {', '.join(query2_sql.group_by)}) AS t2"
        )

        # Combine SELECT statements
        def format_column(column: str, table: str) -> str:
            if ' AS ' in column:
                column = column.split(' AS ')[1].strip()
            return f'{table}.{column}'

        select_clause = (
            ', '.join([format_column(col, 't1') for col in query1_sql.select]) +
            ', ' +
            ', '.join([format_column(col, 't2') for col in query2_sql.select])
        )

        # Special case: dynamically map name columns
        def resolve_name_join(column):
            return f't1.{column} = t2.{column}'

        # JOIN clause with special case for name columns
        on_conditions = [resolve_name_join(col) for col in self.merge_on]
        on_clause = f'ON {" AND ".join(on_conditions)}'

        # ORDER BY clause
        order_by_clause = (
            f'ORDER BY {", ".join(query1_sql.order_by)}'
            if query1_sql.order_by
            else ""
        )

        # Build the full query
        query = (
            f'SELECT {select_clause} '
            f'FROM {pre_aggregated_query1} '
            f'{self.join_type} JOIN {pre_aggregated_query2} {on_clause} '
            f'{order_by_clause}'
        ).strip()
        print(query)
        return query


    def get_args(self) -> List[str]:
        if self.query1 and not self.query2:
            return self.query1.get_args()
        elif self.query2 and not self.query1:
            return self.query2.get_args()
        return self.query1.get_args() + self.query2.get_args()
