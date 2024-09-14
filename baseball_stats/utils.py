import sqlite3
import re
import os
from typing import List, Iterable, Any
from .errors import *

home_dir = os.path.expanduser('~')
db_dir = os.path.join(home_dir, 'baseball_db')
os.makedirs(db_dir, exist_ok=True)
DB_DIR = db_dir
DB_PATH = os.path.join(db_dir, 'baseball_plays.db')


def camel_to_snake(camel_case: str) -> str:
    return re.sub(r'(?<!^)(?=[A-Z])', '_', camel_case).lower()


def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}


def connect() -> tuple[sqlite3.Connection, sqlite3.Cursor]:
    conn = sqlite3.connect(DB_PATH, timeout=680)
    cursor = conn.cursor()
    return conn, cursor


def create_league_average_table() -> None:
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
    except Exception as e:
        raise QueryExecutionError(message=e, query1=query)
    finally:
        conn.close()


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
