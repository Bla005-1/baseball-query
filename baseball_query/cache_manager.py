import time
from typing import List, Tuple, Dict
from .metric_manager import DBMetric
from .async_db import DBManager


class ConstantsCache:
    _instance = None  # Class-level variable to store the single instance

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, db_manager: DBManager, ttl: int = 3600):
        if not hasattr(self, 'cache'):
            self.db_manager = db_manager
            self.ttl = ttl
            self.cache = {}

    def get_cache_entry(self, key: str):
        cache_entry = self.cache.get(key)
        if cache_entry and time.time() - cache_entry['timestamp'] < self.ttl:
            return cache_entry
        return None

    async def get_metric_from_cache(self, key: str, query, column_name) -> List[str] | None:
        cache_entry = self.get_cache_entry(key)
        if cache_entry:
            return cache_entry['data']
        data = await self.db_manager.get_column_values(query, column_name)
        self.cache[key] = {
            'data': data,
            'timestamp': time.time()
        }
        return self.cache[key]['data']

    # Getters for the constants
    async def get_totals_batter(self) -> List[str]:
        query = 'SELECT metric_name FROM metrics WHERE is_totals_batter = 1'
        return await self.get_metric_from_cache('TOTALS_BATTER', query, 'metric_name')

    async def get_totals_pitcher(self) -> List[str]:
        query = 'SELECT metric_name FROM metrics WHERE is_totals_pitcher = 1'
        return await self.get_metric_from_cache('TOTALS_PITCHER', query, 'metric_name')

    async def get_plays_metrics(self) -> List[str]:
        query = 'SELECT metric_name FROM metrics WHERE is_all_plays = 1'
        return await self.get_metric_from_cache('PLAYS_METRICS', query, 'metric_name')

    async def get_group_metrics(self) -> List[str]:
        query = 'SELECT metric_name FROM metrics WHERE is_grouping = 1'
        return await self.get_metric_from_cache('GROUP_METRICS', query, 'metric_name')

    async def get_metrics_dict(self) -> Dict[str, DBMetric]:
        key = 'DB_METRICS'
        cache_entry = self.get_cache_entry(key)
        if cache_entry:
            return cache_entry['data']
        metrics = {}
        data = await self.db_manager.fetch_all('SELECT * FROM metrics')
        for row in data:
            metrics[row['metric_name']] = DBMetric(row)
        self.cache[key] = {
            'data': metrics,
            'timestamp': time.time()
        }
        return metrics

    @staticmethod
    def get_tables() -> Tuple[str, ...]:
        return 'league_averages', 'hitters', 'pitchers', 'fielders', 'all_plays'

    async def get_table_columns_dict(self):
        key = 'TABLE_COLUMNS'
        cache_entry = self.get_cache_entry(key)
        if cache_entry:
            return cache_entry['data']
        table_columns = {}
        for table in self.get_tables():
            values = await self.db_manager.get_column_values(f'DESCRIBE {table}', 'Field')
            table_columns[table] = values
        self.cache[key] = {
            'data': table_columns,
            'timestamp': time.time()
        }
        return table_columns