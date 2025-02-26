import aiomysql
import os
import pandas as pd
from typing import *
from .errors import QueryExecutionError, EmptyQueryError
from .queries import SingleQueryBuilder
from .abc import BaseDBManager

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'baseball_stats'),
    'charset': os.getenv('DB_CHARSET', 'utf8mb4')
}


class DBManager(BaseDBManager):
    def __init__(self, db_config: Dict[str, str] = None, pool_size: int = 10):
        if db_config is None:
            self.db_config = DB_CONFIG
        else:
            self.db_config = db_config
        self.pool = None
        self.pool_size = pool_size

    async def initialize_pool(self):
        if self.pool is None:
            self.pool = await aiomysql.create_pool(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                db=self.db_config['database'],
                charset=self.db_config['charset'],
                autocommit=True,
                maxsize=self.pool_size
            )

    async def fetch_all(self, query: str, params: Tuple | Dict | List = None) -> List[Dict]:
        await self.initialize_pool()
        async with self.pool.acquire() as connection:
            async with connection.cursor(aiomysql.DictCursor) as cursor:
                try:
                    await cursor.execute(query, params)
                    return await cursor.fetchall()
                except Exception as e:
                    raise QueryExecutionError(message=str(e), query1=query)

    async def execute_update(self, query: str, params: Tuple | Dict = None) -> int:
        await self.initialize_pool()
        async with self.pool.acquire() as connection:
            async with connection.cursor(aiomysql.DictCursor) as cursor:
                try:
                    await cursor.execute(query, params)
                    await connection.commit()
                    return cursor.rowcount
                except Exception as e:
                    raise QueryExecutionError(message=str(e), query1=query)

    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def get_column_values(self, query: str, column_name: str) -> List:
        data = await self.fetch_all(query)
        values = [row[column_name] for row in data]
        return values

    async def fetch_metric_sqls(self, metric_names: List[str]) -> Dict[str, str]:
        if not metric_names:
            return {}
        placeholders = ', '.join(['%s'] * len(metric_names))
        query = f'SELECT metric_name, sql_value FROM metrics WHERE metric_name IN ({placeholders})'
        results = await self.fetch_all(query, metric_names)
        return {row['metric_name']: row['sql_value'] for row in results}

    async def get_combined_data(self, query1: SingleQueryBuilder, query2: SingleQueryBuilder, merge_on: Iterable[str]) -> pd.DataFrame:
        if query1 and query2:
            data1 = await self.fetch_all(query1.get_query(), query1.get_args())
            data2 = await self.fetch_all(query2.get_query(), query2.get_args())
            if len(data1) == 0 or len(data2) == 0:
                return pd.DataFrame()
            df = pd.merge(pd.DataFrame(data1), pd.DataFrame(data2), on=merge_on)
        elif query1:
            data1 = await self.fetch_all(query1.get_query(), query1.get_args())
            df = pd.DataFrame(data1)
        elif query2:
            data2 = await self.fetch_all(query2.get_query(), query2.get_args())
            df = pd.DataFrame(data2)
        else:
            raise EmptyQueryError()
        return df
