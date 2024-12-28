import logging
from typing import *

import numpy as np
import pandas as pd
from .processing import get_combined_data, process_batter_rows, process_pitcher_rows
from .queries import TotalsBuilder, PlaysBuilder
from .db_tools import DB_METRICS_DICT, select_data

log = logging.getLogger(__name__)

class BaseballQuery:
    def __init__(self, metric_keys: Iterable[str], player_type: str):
        self.groups = []
        self.player_type = player_type
        self.all_metrics = list(metric_keys)
        metric_keys = list(metric_keys)
        totals_metrics = []
        plays_metrics = []
        groups = []
        self.supplementary_metrics = []
        self.supplementary_df = pd.DataFrame()
        for metric in metric_keys:
            try:
                m = DB_METRICS_DICT[metric]
            except KeyError:
                log.error(f"Metric {metric} not found")
                continue
            if m.dependencies:
                for dep in m.dependencies:
                    dep_metric = DB_METRICS_DICT[dep]
                    if dep_metric.is_all_plays:
                        self.supplementary_metrics.append(dep)
                    else:
                        metric_keys.append(dep)
            if m.is_grouping:
                groups.append(metric)
            elif player_type == 'batter' and m.is_totals_batter:
                totals_metrics.append(metric)
            elif player_type == 'pitcher' and m.is_totals_pitcher:
                totals_metrics.append(metric)
            elif m.is_all_plays:
                plays_metrics.append(metric)
        if len(totals_metrics) > 0:
            totals_metrics.extend(groups)
        if len(plays_metrics) > 0:
            plays_metrics.extend(groups)
        self.total_query = TotalsBuilder(totals_metrics, player_type)
        self.play_query = PlaysBuilder(plays_metrics, player_type)
        for g in groups:
            self.add_group_column(g)

    def add_where_and_group(self, column: str, value: str | List[str]) -> None:  # for user defined columns
        self.add_filters({column: value})
        self.add_group_column(column)

    def add_group_column(self, column: str) -> None:
        if column not in self.groups:
            self.total_query.add_group_column(column)
            self.groups.append(column)
            if column == 'pitch_name' or column == 'inning':
                self.total_query.empty = True
            elif column == 'name':
                self.add_group_column('player_id')
                column = self.play_query.name_column
            elif column == 'team_name':
                column = self.play_query.team_column
            self.play_query.add_group_column(column)

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

    def get_merge(self) -> List[str]:
        return self.groups

    def fetch_data(self) -> pd.DataFrame:
        df = get_combined_data(self.total_query, self.play_query, merge_on=self.get_merge())
        depend = list(set(self.supplementary_metrics))
        if depend:
            depend.extend(self.groups)
            supp_builder = PlaysBuilder(depend, self.player_type)
            supp_builder.sql_query.where = list(self.play_query.sql_query.where)
            supp_builder.args = list(self.play_query.args)
            supplementary_data = select_data(supp_builder.get_query(), supp_builder.get_args())
            self.supplementary_df = pd.DataFrame(supplementary_data)
            self.supplementary_df.fillna(np.nan)
        if df.empty:
            return df
        if self.player_type == 'batter':
            return process_batter_rows(df, self.all_metrics, self.groups, self.supplementary_df)
        elif self.player_type == 'pitcher':
            return process_pitcher_rows(df, self.all_metrics, self.groups, self.supplementary_df)

    def __str__(self):
        return self.play_query.get_query() + '\n' + self.total_query.get_query()
