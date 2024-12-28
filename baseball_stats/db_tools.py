import pymysql
from pymysql.cursors import DictCursor
import os
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


def connect() -> tuple[pymysql.connections.Connection, DictCursor]:
    conn = pymysql.connect(**DB_CONFIG, cursorclass=DictCursor)
    cursor = conn.cursor()
    return conn, cursor


def select_data(query: str, args: Tuple[str] | List[str] | Dict = None) -> Tuple[Dict, ...]:
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


TOTALS_BATTER = select_key_values('SELECT metric_name FROM metrics WHERE is_totals_batter = 1',
                                  'metric_name')
TOTALS_PITCHER = select_key_values('SELECT metric_name FROM metrics WHERE is_totals_pitcher = 1',
                                   'metric_name')
PLAYS_METRICS = select_key_values('SELECT metric_name FROM metrics WHERE is_all_plays = 1', 'metric_name')
GROUP_METRICS = select_key_values('SELECT metric_name FROM metrics WHERE is_grouping = 1', 'metric_name')

def initialize_table_columns(tables: List[str]) -> Dict[str, List[str]]:
    conn, cursor = connect()
    table_columns = {}
    try:
        for table in tables:
            cursor.execute(f"DESCRIBE {table}")
            columns = [row['Field'] for row in cursor.fetchall()]
            table_columns[table] = columns
    finally:
        cursor.close()
        conn.close()
    return table_columns

TABLES = ['league_averages', 'hitters', 'pitchers', 'fielders', 'all_plays']
TABLE_COLUMNS_DICT = initialize_table_columns(TABLES)

def initialize_db_metrics():
    metrics = {}
    data = select_data('SELECT * FROM metrics')
    for row in data:
        metrics[row['metric_name']] = DBMetric(row)
    return metrics

DB_METRICS_DICT = initialize_db_metrics()