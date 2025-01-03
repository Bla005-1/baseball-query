import math
import numpy as np
from typing import List, Dict, Tuple
from pymysql import Connection, MySQLError
from pymysql.cursors import Cursor
from .db_tools import connect, constants_cache


class InsertManager:
    def __init__(self, connection: Tuple[Connection, Cursor] | None = None):
        self.conn, self.cursor = connection or connect()

    @staticmethod
    def clean_data(batch_data: List[Dict], columns: List[str]) -> List[Dict]:
        """
        Clean the data before insertion:
        - Replace commas in strings with semicolons.
        - Replace invalid values '.--' with None.
        """
        cleaned_batch = []
        for item in batch_data:
            cleaned_item = {}
            for key, value in item.items():
                if isinstance(value, str):
                    if ',' in value:
                        value = value.replace(',', ';')
                    if '.--' in value:
                        value = None
                cleaned_item[key] = value
            cleaned_item = {col: cleaned_item.get(col, None) for col in columns}
            cleaned_batch.append(cleaned_item)
        return cleaned_batch

    def insert_league_averages(self, league: str, processed_data: List[Dict], keys: List[str]) -> None:
        arrays = {}
        for row in processed_data:
            for key in keys:
                if key in ('name', 'hits', 'pitches_thrown'):
                    continue
                arrays.setdefault(key, []).append(row[key])
        args = []
        query = '''
            INSERT INTO league_averages (league, metric, mean, stddev) VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE mean=VALUES(mean), stddev=VALUES(stddev)
        '''
        for k, value in arrays.items():
            value = [v for v in value if v is not None and not math.isnan(v)]
            avg = sum(value) / len(value) if value else 0
            std_dev = np.std(value) if value else 0
            args.append((league, k, avg, std_dev))
        self.cursor.executemany(query, args)
        self.conn.commit()

    def insert_batch(self, table: str, batch_data: List[Dict]) -> None:
        columns = constants_cache.get_table_columns_dict()[table]
        q_values = ', '.join(['%s'] * len(columns))
        query = f'INSERT IGNORE INTO {table} ({", ".join(columns)}) VALUES ({q_values})'
        cleaned_batch = self.clean_data(batch_data, columns)
        args = [tuple(item[col] for col in columns) for item in cleaned_batch]
        self.conn.autocommit(False)
        try:
            self.cursor.executemany(query, args)
            self.conn.commit()
        except MySQLError as e:
            self.conn.rollback()
            raise RuntimeError(f"Error inserting batch into {table}: {e}")
        finally:
            self.conn.autocommit(True)

    def close(self):
        self.cursor.close()
        self.conn.close()
