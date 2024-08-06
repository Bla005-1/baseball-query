import sqlite3
import re
import os
from typing import List, Iterable, Any
from .static_data import total_metrics, play_metrics, requires_pitch_results

home_dir = os.path.expanduser('~')
db_dir = os.path.join(home_dir, 'baseball_db')
os.makedirs(db_dir, exist_ok=True)
DB_DIR = db_dir
DB_PATH = os.path.join(db_dir, 'baseball_plays.db')


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
                    self.where.append(f'league IN ({", ".join("?"*len(league))})')
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

    def finish_query(self):
        if len(self.where) == 1:
            self.base_query += 'WHERE ' + self.where[0]
        elif len(self.where) > 1:
            self.base_query += 'WHERE ' + ' AND '.join(self.where)
        if self.name is None:
            self.group_by.append(self.name_column)
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
        if add_default:
            empty += 1
            if 'league' not in metrics:
                metrics.append('league')
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
                self.name_column = m
            elif m == 'batter_name':
                self.name_column = m
        if len(self.metrics) > empty:
            self.empty = False
        self.base_query += ', '.join(set(self.metrics))
        self.base_query += ' FROM all_plays '


def camel_to_snake(camel_case):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', camel_case).lower()


def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}


def connect():
    conn = sqlite3.connect(DB_PATH, timeout=680)
    cursor = conn.cursor()
    return conn, cursor


def create_league_average_table():
    query = '''
    CREATE TABLE IF NOT EXISTS league_averages (
        league TEXT NOT NULL,
        metric TEXT NOT NULL,
        mean REAL,
        stddev REAL,
        PRIMARY KEY (league, metric)
    )'''
    conn, cursor = connect()
    cursor.execute(query)
    conn.commit()
    conn.close()


def select_data(query: str, args: Iterable = None, row_factory=dict_factory) -> List[Any]:
    conn, cursor = connect()
    try:
        if row_factory is not None:
            cursor.row_factory = row_factory
        if args is not None:
            cursor.execute(query, args)
        else:
            cursor.execute(query)
        data = cursor.fetchall()
        return data
    finally:
        conn.close()


# old function?
def get_matching_search(name: str, league: str, team: str, dates: tuple, player_type: str) -> List[sqlite3.Row]:
    conditions = [dates[0], dates[1]]
    if player_type == 'pitcher':
        query = f'SELECT DISTINCT pitcher_name, league, team_fielding FROM all_plays WHERE date BETWEEN ? AND ?'
        if name:
            query += ' AND LOWER(pitcher_name) LIKE ?'
            conditions.append('%' + name.lower() + '%')
        if league:
            query += ' AND LOWER(league) = ?'
            conditions.append(league.lower())
        if team:
            query += ' AND LOWER(team_fielding) = ?'
            conditions.append(team.lower())
        query += ' GROUP BY pitcher_name, league'
    else:
        query = f'SELECT DISTINCT batter_name, league, team_batting FROM all_plays WHERE date BETWEEN ? AND ?'
        if name:
            query += ' AND LOWER(batter_name) LIKE ?'
            conditions.append('%' + name.lower() + '%')
        if league:
            query += ' AND LOWER(league) = ?'
            conditions.append(league.lower())
        if team:
            query += ' AND LOWER(team_batting) = ?'
            conditions.append(team.lower())
        query += ' GROUP BY batter_name, league'
    conn, cursor = connect()
    cursor.execute(query, conditions)
    rows = cursor.fetchall()
    conn.close()
    result = []
    current_name = None
    for index, tup in enumerate(rows):
        name, league, team = tup

        if name != current_name:
            if index != len(rows) - 1:
                if rows[index+1][0] == name:
                    result.append((name, '', ''))
            current_name = name

        result.append(tup)

    return result


class DebugManager:
    def __init__(self):
        self.metrics = {}

    def increment(self, category: str, metric: str, value: int = 1):
        self.metrics.setdefault(category, {}).setdefault(metric, 0)
        self.metrics[category][metric] += value

    def __str__(self):
        output = ""
        for category, metrics in self.metrics.items():
            output += f"{category}:\n"
            for metric, value in metrics.items():
                output += f"    {metric.replace('_', ' ').capitalize()}: {value}\n"
        return output
