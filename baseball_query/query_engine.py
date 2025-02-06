from typing import List, Dict, Type
import pandas as pd
import time
from .async_db import DBManager
from .baseball_query import BaseballQuery
from .cache_manager import ConstantsCache
from .queries import BaseQueryBuilder, PlaysBuilder
# from .processing import Processor


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

    async def create_query(self, metrics: List[str], player_type: str,
                           buildr_cls: Type[BaseQueryBuilder] = BaseballQuery) -> BaseQueryBuilder:
        """
        Factory method to create an async BaseballQuery instance.
        """
        metrics_dict = await self.cache.get_metrics_dict()
        builder = buildr_cls(player_type)
        for metric in metrics:
            if metric := metrics_dict.get(metric):
                if metric.metric_name == 'name':
                    if isinstance(builder, BaseballQuery) or isinstance(builder, PlaysBuilder):
                        metrics.append('batter_name' if player_type == 'batter' else 'pitcher_name')
                if metric.is_grouping:
                    builder.add_group_by(metric.metric_name)
                builder.add_select(metric)
        return builder

    async def fetch_data(self, query_builder: BaseQueryBuilder) -> pd.DataFrame:
        start = time.perf_counter()
        data = await self.db_manager.fetch_all(query_builder.get_query(), query_builder.get_args())
        df = pd.DataFrame(data)
        print(df.columns)
        print(f'First fetch took {time.perf_counter() - start} seconds')
        return df
        # p = Processor(self.db_manager)
        # start = time.perf_counter()
        # if query_builder.player_type == 'batter':
        #     d = await p.calculate_batter_rows(df, query_builder.get_metric_names(), query_builder.get_group_columns(), self.supplementary_df)
        #     print(f'Batter calcs took {time.perf_counter() - start} seconds')
        #     return d
        # elif query_builder.player_type == 'pitcher':
        #     d = await p.calculate_pitcher_rows(df, query_builder.get_metric_names(), query_builder.groups, self.supplementary_df)
        #     print(f'Pitcher calcs took {time.perf_counter() - start} seconds')
        #     return d
