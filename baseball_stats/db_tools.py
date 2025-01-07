import pymysql
from pymysql.cursors import DictCursor
import os
import time
from typing import List, Tuple, Dict
from .errors import *
from .metrics_abc import DBMetric

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'baseball_plays'),
    'charset': os.getenv('DB_CHARSET', 'utf8mb4')
}

def update_db_config(config: Dict) -> None:
    global DB_CONFIG
    DB_CONFIG = config

def connect() -> tuple[pymysql.connections.Connection, DictCursor]:
    conn = pymysql.connect(**DB_CONFIG, cursorclass=DictCursor)
    cursor = conn.cursor()
    return conn, cursor


def select_data(query: str, args: Tuple[str] | List[str] | Dict = None) -> Tuple[Dict]:
    conn, cursor = connect()
    try:
        if args is not None:
            cursor.execute(query, args)
        else:
            cursor.execute(query)
        data = cursor.fetchall()
        return data
    except Exception as e:
        raise QueryExecutionError(message=str(e), query1=query)
    finally:
        conn.close()


def select_key_values(query: str, key_name: str) -> List:
    data = select_data(query)
    keys = [x[key_name] for x in data]
    return keys


def fetch_metric_sqls(metric_names: List[str]) -> Dict[str, str]:
    if not metric_names:
        return {}
    placeholders = ', '.join(['%s'] * len(metric_names))
    query = f"SELECT metric_name, sql_value FROM metrics WHERE metric_name IN ({placeholders})"
    results = select_data(query, metric_names)
    return {row['metric_name']: row['sql_value'] for row in results}


class ConstantsCache:
    def __init__(self, cache_expiry: int = 3600):
        self.cache_expiry = cache_expiry
        self.cache = {}

    def get_cache_entry(self, key: str):
        cache_entry = self.cache.get(key)
        if cache_entry and time.time() - cache_entry['timestamp'] < self.cache_expiry:
            return cache_entry
        return None

    def get_metric_from_cache(self, key: str, query, column_name) -> List[str] | None:
        cache_entry = self.get_cache_entry(key)
        if cache_entry:
            return cache_entry['data']
        self.cache[key] = {
            'data': select_key_values(query, column_name),
            'timestamp': time.time()
        }
        return self.cache[key]['data']

    # Getters for the constants
    def get_totals_batter(self) -> List[str]:
        query = 'SELECT metric_name FROM metrics WHERE is_totals_batter = 1'
        return self.get_metric_from_cache('TOTALS_BATTER', query, 'metric_name')

    def get_totals_pitcher(self) -> List[str]:
        query = 'SELECT metric_name FROM metrics WHERE is_totals_pitcher = 1'
        return self.get_metric_from_cache('TOTALS_PITCHER', query, 'metric_name')

    def get_plays_metrics(self) -> List[str]:
        query = 'SELECT metric_name FROM metrics WHERE is_all_plays = 1'
        return self.get_metric_from_cache('PLAYS_METRICS', query, 'metric_name')

    def get_group_metrics(self) -> List[str]:
        query = 'SELECT metric_name FROM metrics WHERE is_grouping = 1'
        return self.get_metric_from_cache('GROUP_METRICS', query, 'metric_name')

    def get_db_metrics_dict(self):
        key = 'DB_METRICS'
        cache_entry = self.get_cache_entry(key)
        if cache_entry:
            return cache_entry['data']
        metrics = {}
        data = select_data('SELECT * FROM metrics')
        for row in data:
            metrics[row['metric_name']] = DBMetric(row)
        self.cache[key] = {
            'data': metrics,
            'timestamp': time.time()
        }
        return metrics

    @staticmethod
    def get_tables() -> Tuple[str]:
        return 'league_averages', 'hitters', 'pitchers', 'fielders', 'all_plays'

    def get_table_columns_dict(self):
        key = 'TABLE_COLUMNS'
        cache_entry = self.get_cache_entry(key)
        if cache_entry:
            return cache_entry['data']
        table_columns = {}
        for table in self.get_tables():
            rows = select_data(f'DESCRIBE {table}')
            columns = [row['Field'] for row in rows]
            table_columns[table] = columns
        self.cache[key] = {
            'data': table_columns,
            'timestamp': time.time()
        }
        return table_columns

constants_cache = ConstantsCache()
