from typing import List, Dict
from .db_access_layer import DBManager
from .main_query_builder import BaseballQuery
from .cache import ConstantsCache


class BaseballStats:
    def __init__(self, db_config: Dict = None, pool_size: int = 10):
        """
        Initialize the async BaseballStats.
        :param db_config: Database configuration dictionary.
        :param pool_size: Connection pool size for async operation.
        """
        self.db_manager = DBManager(db_config, pool_size)
        self.cache = ConstantsCache(self.db_manager)

    async def initialize(self):
        await self.db_manager.initialize_pool()

    async def close(self):
        """
        Clean up resources (e.g., close async pools).
        """
        await self.db_manager.close()

    async def create_query(self, metrics: List[str], player_type: str) -> BaseballQuery:
        """
        Factory method to create an async BaseballQuery instance.
        """
        metrics_dict = await self.cache.get_metrics_dict()
        return BaseballQuery(metrics, player_type, self.db_manager, metrics_dict)
