from typing import List
from .static_data import *


class QueryBuilder:
    def __init__(self, base_query: str = 'SELECT '):
        self.empty = True
        self.base_query = base_query
        self.args = []
        self.where = []
        self.order = []
        self.name_column = 'name'
        self.team_column = 'team_name'
        self.group_by = []
        self.is_finished = False

    def add_name(self, name):
        if name:
            if isinstance(name, str) or (isinstance(name, list) and len(name) == 1):
                self.where.append(f'{self.name_column} = ?')
                self.args.append(name if isinstance(name, str) else name[0])
            else:
                name = [f'"{x}"' for x in name]
                self.where.append(f'{self.name_column} IN ({", ".join(name)})')
                self.group_by.append(self.name_column)
        else:
            self.group_by.append(self.name_column)
        return self

    def add_team(self, team):
        if team:
            if isinstance(team, str) or (isinstance(team, list) and len(team) == 1):
                self.where.append(f'{self.team_column} = ?')
                self.args.append(team if isinstance(team, str) else team[0])
            else:
                team = [f'"{x}"' for x in team]
                self.where.append(f'{self.team_column} IN ({", ".join(team)})')
                self.group_by.append(self.team_column)
        else:
            self.group_by.append(self.name_column)
        return self

    def add_all_but_name(self, league: str | List[str] = None, dates=None, year: str = None,
                         game_type: str | List[str] = None):
        self.extra_where('league', league)
        self.add_dates(dates)
        self.add_year(year)
        self.extra_where('game_type', game_type)

    def add_dates(self, dates):
        if dates is not None:
            if len(dates) == 2:
                if dates[0] and dates[1]:
                    self.add_other_where('date BETWEEN ? AND ?', [dates[0], dates[1]])

    def add_year(self, year: str):
        if year:
            self.where.append('date LIKE ?')
            self.args.append(year + '%')

    def add_other_where(self, where_clause: str, args: List[str] | str = None):
        self.where.append(where_clause)
        if isinstance(args, str):
            self.args.append(args)
        elif args:
            self.args.extend(args)

    def order_by(self, column: str):
        self.order.append(column)

    def extra_group(self, column: str | List[str]):
        if isinstance(column, str):
            self.group_by.append(column)
        else:
            self.group_by.extend(column)

    def extra_where(self, column: str, values: str | List[str]):
        if not values:
            self.extra_group(column)
            return
        if isinstance(values, list):
            if len(values) == 1:
                self.where.append(f'{column} = ?')
            else:
                self.where.append(f'{column} IN ({", ".join("?" * len(values))})')
            self.args.extend(values)
        else:
            self.where.append(f'{column} = ?')
            self.args.append(values)

    def finish_query(self) -> str:
        if self.is_finished:
            return self.get_query()
        if self.where:
            self.base_query += ' WHERE ' + ' AND '.join(self.where)
        if self.group_by:
            self.base_query += f' GROUP BY {", ".join(self.group_by)}'
        if self.order:
            self.base_query += f' ORDER BY {", ".join(self.order)}'
        self.is_finished = True
        return self.get_query()

    def add_other(self, clause: str):
        self.where.append(clause)

    def get_query(self):
        return self.base_query

    def get_args(self):
        return self.args

    def __str__(self):
        return self.base_query

    def __bool__(self):
        return not self.empty


def append_or_extend(the_list: List, the_item: List[str] | str):
    if isinstance(the_item, str):
        the_list.append(the_item)
    else:
        the_list.extend(the_item)


class TotalsBuilder(QueryBuilder):
    def __init__(self, metric_keys: List[str], player_type: str):
        super().__init__()
        self.metrics = []
        self.name_column = 'name'
        self.team_column = 'team_name'
        for m in metric_keys:
            if m in totals_common.keys():
                append_or_extend(self.metrics, totals_common[m])
            elif player_type == 'batter' and m in totals_batter_metrics:
                append_or_extend(self.metrics, totals_batter_metrics[m])
            elif player_type == 'pitcher' and m in totals_pitcher_metrics:
                append_or_extend(self.metrics, totals_pitcher_metrics[m])
            elif m in grouping_columns:
                append_or_extend(self.metrics, m)
        if metric_keys and self.metrics:
            if not all(item in grouping_columns for item in self.metrics):
                self.empty = False
        self.base_query += ', '.join(set(self.metrics))
        if player_type == 'batter':
            table = 'hitters'
        else:
            table = 'pitchers'
        self.base_query += f' FROM {table} '


class PlaysBuilder(QueryBuilder):
    def __init__(self, metric_keys: List[str], player_type: str):
        super().__init__()

        self.name_column = player_type + '_name'
        self.team_column = 'team_batting' if player_type == 'batter' else 'team_fielding'
        self.metrics = []
        for m in metric_keys:
            if m == 'team_name':
                m = self.team_column
            elif m == 'name':
                m = self.name_column
            if m in play_metrics.keys():
                append_or_extend(self.metrics, play_metrics[m])
            elif m in requires_pitch_results:
                append_or_extend(self.metrics, play_metrics['pitch_results'])
            elif m in grouping_columns:
                append_or_extend(self.metrics, m)
        if metric_keys and self.metrics:
            if not all(item in grouping_columns for item in self.metrics):
                self.empty = False
        self.base_query += ', '.join(set(self.metrics))
        self.base_query += ' FROM all_plays '



