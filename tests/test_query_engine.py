import pandas as pd
import pytest
from baseball_query.query_engine import BaseballQueryClient
from baseball_query.abc import DBMetric, BaseDBManager


class FakeDBManager(BaseDBManager):
    """Test double that records and returns predefined query data."""

    def __init__(self, return_data):
        # return_data may be a list or dict mapping query string to data
        self.return_data = return_data
        self.queries = []

    async def initialize_pool(self):
        pass

    async def fetch_all(self, query: str, params=None):
        self.queries.append((query, params))
        if isinstance(self.return_data, dict):
            return self.return_data.get(query, [])
        return self.return_data

    async def execute_update(self, query: str, params=None):
        return 0

    async def close(self):
        pass

    async def get_column_values(self, query: str, column_name: str):
        rows = await self.fetch_all(query)
        return [row[column_name] for row in rows]

    async def fetch_metric_sqls(self, metric_names):
        return {name: name for name in metric_names}

    async def get_combined_data(self, query1, query2, merge_on):
        data1 = await self.fetch_all(query1.get_query(), query1.get_args()) if query1 else []
        data2 = await self.fetch_all(query2.get_query(), query2.get_args()) if query2 else []
        if query1 and query2:
            if not data1 or not data2:
                import pandas as pd
                return pd.DataFrame()
            import pandas as pd
            return pd.merge(pd.DataFrame(data1), pd.DataFrame(data2), on=merge_on)
        elif query1:
            import pandas as pd
            return pd.DataFrame(data1)
        elif query2:
            import pandas as pd
            return pd.DataFrame(data2)
        else:
            from baseball_query.errors import EmptyQueryError
            raise EmptyQueryError()


class FakeCache:
    """Minimal cache stub supplying metrics for tests."""

    def __init__(self, metrics_dict):
        self.metrics_dict = metrics_dict

    async def get_metrics_dict(self):
        return self.metrics_dict


def test_create_query_and_fetch():
    metrics_dict = {
        'hits': DBMetric({'metric_name': 'hits', 'sql_value': 'hits', 'is_totals_batter': 1})
    }
    client = BaseballQueryClient()
    client.db_manager = FakeDBManager([{'hits': 5}])
    client.cache = FakeCache(metrics_dict)
    import asyncio
    builder = asyncio.run(client.create_query(['hits'], player_type='batter'))
    df = asyncio.run(client.fetch_data(builder, skip_processor=True))
    assert isinstance(df, pd.DataFrame)
    assert df.iloc[0]['hits'] == 5
    assert client.db_manager.queries  # ensure query was executed

def test_get_combined_data_merge():
    # Two separate queries returning different datasets merging on 'id'
    from baseball_query.queries import SingleQueryBuilder
    from baseball_query.sql_query import SQLQuery

    qb1 = SingleQueryBuilder('batter', SQLQuery())
    qb1.set_table('t1')
    qb1.sql_query.add_select('id')
    qb1.sql_query.add_select('a')

    qb2 = SingleQueryBuilder('pitcher', SQLQuery())
    qb2.set_table('t2')
    qb2.sql_query.add_select('id')
    qb2.sql_query.add_select('b')

    return_data = {
        qb1.get_query(): [{'id': 1, 'a': 'x'}],
        qb2.get_query(): [{'id': 1, 'b': 'y'}]
    }
    db = FakeDBManager(return_data)
    import asyncio
    df = asyncio.run(db.get_combined_data(qb1, qb2, merge_on=['id']))
    assert df.to_dict('records') == [{'id': 1, 'a': 'x', 'b': 'y'}]

