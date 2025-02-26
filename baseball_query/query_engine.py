from typing import List, Dict, Type, Optional, overload
import pandas as pd
import time
from .async_db import DBManager
from .cache_manager import ConstantsCache
from .queries import PlaysBuilder, TotalsBuilder
from .abc import BaseQueryFactory, BuilderT
from .processing import Processor


class BaseballQueryClient(BaseQueryFactory):
    def __init__(self, db_config: Dict = None, pool_size: int = 10):
        """
        Initialize the async BaseballStats.
        :param db_config: Database configuration dictionary.
        :param pool_size: Connection pool size for async operation.
        """
        self.db_manager = DBManager(db_config, pool_size)
        self.cache = ConstantsCache(self.db_manager)
        self._initialized = False

    async def initialize(self):
        await self.db_manager.initialize_pool()
        self._initialized = True

    async def close(self):
        """
        Clean up resources (e.g., close async pools).
        """
        await self.db_manager.close()

    @overload
    async def create_query(self, metrics: List[str], player_type: str, builder_cls: None = None) -> TotalsBuilder:
        ...

    @overload
    async def create_query(
            self,
            metrics: List[str],
            player_type: str,
            builder_cls: Type[BuilderT] = None
    ) -> BuilderT:
        ...

    async def create_query(
            self,
            metrics: List[str],
            player_type: str,
            builder_cls: Optional[Type[BuilderT]] = None
    ) -> BuilderT:
        if builder_cls is None:
            builder_cls = TotalsBuilder
        metrics_dict = await self.cache.get_metrics_dict()
        builder = builder_cls(player_type)

        def add_metric(metric):
            if metric := metrics_dict.get(metric):
                if metric.dependencies:
                    for dependency in metric.dependencies:
                        add_metric(dependency)
                if metric.metric_name == 'name':
                    if isinstance(builder, PlaysBuilder):
                        metrics.append('batter_name' if player_type == 'batter' else 'pitcher_name')
                if metric.is_grouping:
                    builder.group_by(metric.metric_name)
                builder.add_select(metric)

        for user_metric in metrics:
            add_metric(user_metric)
        return builder

    async def fetch_data(self, query_builder: BuilderT) -> pd.DataFrame:
        start = time.perf_counter()
        data = await self.db_manager.fetch_all(query_builder.get_query(), query_builder.get_args())
        df = pd.DataFrame(data)
        print(f'First fetch took {time.perf_counter() - start} seconds')
        p = Processor(query_builder, self)
        start = time.perf_counter()
        if query_builder.player_type == 'batter':
            d = await p.calculate_batter_rows(df)
            print(f'Batter calcs took {time.perf_counter() - start} seconds')
            return d
        elif query_builder.player_type == 'pitcher':
            d = await p.calculate_pitcher_rows(df)
            print(f'Pitcher calcs took {time.perf_counter() - start} seconds')
            return d
        else:
            raise ValueError(f'Unknown player type: {query_builder.player_type}')
