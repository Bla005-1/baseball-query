from typing import List, Tuple, Self, Dict
from .errors import EmptyQueryError
from .sql_query import SQLQuery
from .abc import BaseQueryBuilder, DBMetric


class SingleQueryBuilder(BaseQueryBuilder):
    def __init__(self, player_type: str = '', sql_query: SQLQuery = None):
        super().__init__(player_type)
        self.sql_query = sql_query or SQLQuery()
        self.args = []
        self.name_column = 'name'
        self.team_column = 'team_name'

    def set_table(self, table: str):
        self.sql_query.set_from_table(table)

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

    def add_filters(self, filters: Dict) -> Self:
        for column, value in filters.items():
            if value:
                if column == 'name':
                    self.add_name(value)
                elif column == 'team_name':
                    self.add_team(value)
                elif column == 'start_date':
                    self.add_dates((value, filters.get('end_date')))
                elif column == 'end_date':
                    continue
                elif column == 'year':
                    self.add_year(value)
                else:
                    self.add_dynamic_where(column, value)

    def add_raw_where(self, where_clause: str, args: List[str] | str = None) -> Self:
        self.sql_query.add_where(where_clause)
        if isinstance(args, str):
            self.args.append(args)
        elif args:
            self.args.extend(args)
        return self

    def order_by(self, column: str | List[str]) -> Self:
        if isinstance(column, str):
            self.sql_query.add_order_by(column)
        else:
            for c in column:
                self.sql_query.add_order_by(c)
        return self

    def group_by(self, column: str | List[str]) -> Self:
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
            self.group_by(column)
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

    def update_selects(self, e: str):
        e, alias = self._parse_select(e)
        self.sql_query.add_select(e)
        self.metric_names.append(alias)

    def get_where_clauses(self) -> List[str]:
        return self.sql_query.where

    def add_select(self, metric: DBMetric) -> Self:
        if metric.is_python:
            self.python_metrics.append(metric.metric_name)
            return self
        expression = metric.sql_value
        if '!!' in expression:  # might need to change to ! or ~
            values = expression.split('!!')
            for v in values:
                self.update_selects(v)
        elif expression:
            self.update_selects(expression)
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
        self.set_table('hitters' if player_type == 'batter' else 'pitchers')

    def add_select(self, metric: DBMetric) -> Self:
        if ((metric.is_totals_batter and self.player_type == 'batter') or
            (metric.is_totals_pitcher and self.player_type == 'pitcher') or
            metric.is_python):
            return super().add_select(metric)
        return self

class PlaysBuilder(SingleQueryBuilder):
    def __init__(self, player_type: str):
        super().__init__(player_type)
        self.name_column = player_type + '_name'
        self.team_column = 'team_batting' if player_type == 'batter' else 'team_fielding'
        self.set_table('all_plays')

    def add_select(self, metric: DBMetric) -> Self:
        if metric.is_all_plays or metric.is_python:
            return super().add_select(metric)
        return self
