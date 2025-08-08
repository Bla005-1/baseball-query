import pandas as pd
import asyncio

from baseball_query.processing import Processor


class DummyQueryBuilder:
    player_type = 'batter'
    python_metrics = ['OPS']

    def get_metric_names(self):
        return ['OBP', 'SLG']


class DummyFactory:
    def __init__(self):
        self.db_manager = None


def test_ops_calculation():
    df = pd.DataFrame([{'OBP': 0.3, 'SLG': 0.5}])
    processor = Processor(DummyQueryBuilder(), DummyFactory())
    async def _dummy_apply_per_row(df):
        return df

    processor.apply_per_row = _dummy_apply_per_row  # type: ignore
    result = asyncio.run(processor.calculate_batter_rows(df))
    assert 'OPS' in result.columns
    assert result.iloc[0]['OPS'] == 0.8
