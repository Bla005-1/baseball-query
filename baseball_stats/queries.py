from .static_data import total_metrics, play_metrics, requires_pitch_results
from typing import List


class QueryBuilder:
    def __init__(self, base_query: str = 'SELECT '):
        self.empty = True
        self.base_query = base_query
        self.args = []
        self.where = []
        self.order = None
        self.name = None
        self.name_column = 'name'
        self.group_by = []

    def add_name(self, name):
        self.name = name
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

    def add_all_but_name(self, league: str | List[str] = None, dates=None, year: str = None, game_type: str = None):
        self.add_league(league)
        self.add_dates(dates)
        self.add_year(year)
        self.add_game_type(game_type)

    def add_league(self, league: str | List[str]):
        if league:
            if isinstance(league, str):
                self.where.append('league = ?')
                self.args.append(league)
            else:
                if len(league) == 1:
                    self.where.append('league = ?')
                    self.args.append(league[0])
                else:
                    self.where.append(f'league IN ({", ".join("? " *len(league))})')
                    self.args.extend(league)
                    self.group_by.append('league')

    def add_dates(self, dates):
        if dates:
            self.where.append('date BETWEEN ? AND ?')
            self.args.extend([dates[0], dates[1]])

    def add_year(self, year: str):
        if year:
            self.where.append('date LIKE ?')
            self.args.append(year + '%')

    def add_game_type(self, game_type: str):
        if game_type:
            self.where.append('game_type = ?')
            self.args.append(game_type)

    def add_other(self, where_clause: str):
        self.where.append(where_clause)

    def order_by(self, column: str):
        self.order = f' ORDER BY {column}'

    def extra_group(self, column: str | List[str]):
        if isinstance(column, str):
            self.group_by.append(column)
        else:
            self.group_by.extend(column)

    def extra_where(self, column: str, values: str | List[str]):
        if isinstance(values, list):
            if len(values) == 1:
                self.where.append(f'{column} = ?')
            else:
                self.where.append(f'{column} IN ({", ".join("?" * len(values))})')
            self.args.extend(values)
        else:
            self.where.append(f'{column} = ?')
            self.args.append(values)

    def finish_query(self):
        if len(self.where) == 1:
            self.base_query += 'WHERE ' + self.where[0]
        elif len(self.where) > 1:
            self.base_query += 'WHERE ' + ' AND '.join(self.where)
        if self.group_by:
            self.base_query += f' GROUP BY {", ".join(self.group_by)}'
        if self.order is not None:
            self.base_query += self.order

    def get_query(self):
        return self.base_query

    def get_args(self):
        return self.args

    def __str__(self):
        return self.base_query

    def __bool__(self):
        return not self.empty


class TotalsBuilder(QueryBuilder):
    def __init__(self, metrics: List[str], player_type: str, add_default=True):
        super().__init__()
        self.init_metrics = list(metrics)
        empty = 0
        if add_default:
            empty += 3
            self.defaults()
        self.metrics = []
        self.name_column = 'name'
        for m in self.init_metrics:
            if m in total_metrics.keys():
                self.metrics.append(total_metrics[m])
        if len(self.metrics) > empty:
            self.empty = False
        self.base_query += ', '.join(set(self.metrics))
        self.base_query += f' FROM {player_type} '

    def defaults(self):
        for default_metric in ('name', 'league', 'player_id'):
            if default_metric not in self.init_metrics:
                self.init_metrics.append(default_metric)


class PlaysBuilder(QueryBuilder):
    def __init__(self, metrics: List[str], name_column=None, add_default=True):
        super().__init__()
        if name_column:
            self.name_column = name_column
        metrics = list(metrics)
        empty = 0
        if 'league' not in metrics:
            if add_default:
                metrics.append('league')
        else:
            empty += 1
        self.metrics = []
        for m in metrics:
            if m in play_metrics.keys():
                sql = play_metrics[m]
                if isinstance(sql, list):
                    self.metrics.extend(sql)
                else:
                    self.metrics.append(sql)
            elif m in requires_pitch_results:
                self.metrics.append(play_metrics['pitch_results'])
            if m == 'pitcher_name':
                empty += 1
                self.name_column = m
            elif m == 'batter_name':
                empty += 1
                self.name_column = m
        if len(self.metrics) > empty:
            self.empty = False
        self.base_query += ', '.join(set(self.metrics))
        self.base_query += ' FROM all_plays '
