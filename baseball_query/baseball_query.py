import logging
from typing import *
import pandas as pd
from .queries import TotalsBuilder, PlaysBuilder, SQLJoinBuilder, BaseQueryBuilder
from .metric_manager import DBMetric


log = logging.getLogger(__name__)


class BaseballQuery(BaseQueryBuilder):
    def __init__(self, player_type: str):
        super().__init__(player_type)
        self.supplementary_metrics = []
        self.supplementary_df = pd.DataFrame()
        self.total_query = TotalsBuilder(player_type)
        self.play_query = PlaysBuilder(player_type)

    def add_select(self, metric: DBMetric) -> Self:
        if self.player_type == 'batter' and metric.is_totals_batter:
            self.total_query.add_select(metric)
        elif self.player_type == 'pitcher' and metric.is_totals_pitcher:
            self.total_query.add_select(metric)
        if metric.is_all_plays:
            self.play_query.add_select(metric)
        return self


    def add_where_and_group(self, column: str, value: str | List[str]) -> Self:  # for user defined columns
        self.add_filters({column: value})
        self.add_group_by(column)
        return self

    def add_group_by(self, column: str | List[str]) -> Self:
        def _add_groups(c: str):
            if c not in self.group_columns:
                self.total_query.add_group_by(c)
                self.group_columns.append(c)
                if c == 'pitch_name' or c == 'inning':
                    self.total_query.empty = True
                elif c == 'name':
                    self.add_group_by('player_id')
                self.play_query.add_group_by(c)

        if isinstance(column, str):
            _add_groups(column)
        elif isinstance(column, list):
            for col in column:
                _add_groups(col)
        return self

    def order_by(self, column: str) -> None:
        self.total_query.order_by(column)
        if column == 'name':
            column = self.play_query.name_column
        elif column == 'team_name':
            column = self.play_query.team_column
        self.play_query.order_by(column)

    def add_filters(self, filters: Dict) -> None:
        for build in (self.total_query, self.play_query):
            for column, values in filters.items():
                if values:
                    if column == 'start_date':
                        build.add_dates((values, filters.get('end_date')))
                    elif column == 'end_date':
                        continue
                    elif column == 'year':
                        build.add_year(values)
                    elif column == 'name':
                        build.add_name(values)
                    elif column == 'team_name':
                        build.add_team(values)
                    else:
                        build.add_dynamic_where(column, values)

    def get_query(self) -> str:
        joiner = SQLJoinBuilder(self.total_query, self.play_query, self.get_group_columns())
        return joiner.build_join_query()

    def get_args(self):
        if self.play_query and not self.total_query:
            return self.play_query.get_args()
        elif self.total_query and not self.play_query:
            return self.play_query.get_args()
        return self.play_query.get_args() + self.total_query.get_args()
