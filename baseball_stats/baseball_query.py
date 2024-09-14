from typing import *
from .common_data import get_combined_data
from .builder_metrics import *
from .queries import TotalsBuilder, PlaysBuilder
from .batter_data import process_batter_rows
from .pitch_data import process_pitcher_rows

all_total_keys = list(totals_common.keys()) + list(totals_batter_metrics.keys()) + list(totals_pitcher_metrics.keys())


class BaseballQuery:
    def __init__(self, metric_keys: List[str], player_type: str):
        self.groups = []
        self.player_type = player_type
        self.all_metrics = metric_keys
        totals_metrics = []
        plays_metrics = []
        for metric in metric_keys:
            if metric in set(grouping_columns):
                self.groups.append(metric)
            elif metric in set(all_total_keys):
                totals_metrics.append(metric)
            elif metric in set(list(play_metrics.keys()) + requires_pitch_results):
                plays_metrics.append(metric)
        if totals_metrics:
            totals_metrics.extend(self.groups)
        if plays_metrics:
            plays_metrics.extend(self.groups)
        self.total_query = TotalsBuilder(totals_metrics, player_type)
        self.play_query = PlaysBuilder(plays_metrics, player_type)
        for i, group in enumerate(self.groups):
            if group == 'pitch_name':
                self.total_query.empty = True
            elif group == 'name':
                group = 'player_id'
                self.groups[i] = group
            self.total_query.add_group_column(group)
            if group == 'team_name':
                group = self.play_query.team_column

            self.play_query.add_group_column(group)

    def add_where_and_group(self, column: str, value: str | List[str]):  # for user defined columns
        self.add_filters({column: value})
        self.add_group_column(column)

    def add_group_column(self, column):
        if column not in self.groups:
            self.total_query.add_group_column(column)
            self.groups.append(column)
            if column == 'name':
                column = self.play_query.name_column
            elif column == 'team_name':
                column = self.play_query.team_column
            self.play_query.add_group_column(column)


    def order_by(self, column):
        self.total_query.order_by(column)
        if column == 'name':
            column = self.play_query.name_column
        elif column == 'team_name':
            column = self.play_query.team_column
        self.play_query.order_by(column)

    def add_filters(self, filters: Dict):
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

    def get_merge(self):
        return self.groups

    def fetch_data(self):
        df = get_combined_data(self.total_query, self.play_query, merge_on=self.get_merge())
        if df.empty:
            return df
        if self.player_type == 'batter':
            return process_batter_rows(df, self.all_metrics)
        if self.player_type == 'pither':
            return process_pitcher_rows(df, self.all_metrics)

    def __str__(self):
        return self.play_query.get_query() + '\n' + self.total_query.get_query()
