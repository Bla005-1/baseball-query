from abc import ABC, abstractmethod
from typing import List, Tuple, Dict
import pandas as pd
import numpy as np
import asyncio


class VectorizedMetric(ABC):
    def __init__(self, names: str | List[str], dependencies: Tuple[str, ...]):
        if isinstance(names, str):
            names = [names]
        self.names = names
        self.dependencies = dependencies
        self.requires_row = False
        self.original_row = pd.Series()

    @abstractmethod
    def calculate(self, temp_df: pd.DataFrame) -> dict:
        """Calculate the metric using vectorized operations."""
        pass

    def add_row(self, row: pd.Series):
        self.original_row = row

class IterationMetric(ABC):
    def __init__(self, names: str | List[str], dependencies: Tuple[str, ...]):
        if isinstance(names, str):
            names = [names]
        self.names = names
        self.dependencies = dependencies

    @abstractmethod
    def reset(self):
        """Reset internal state before processing."""
        pass

    @abstractmethod
    def update(self, i, items):
        """Update internal state during iteration."""
        pass

    @abstractmethod
    def finalize(self) -> dict:
        """Finalize metric calculations and return results."""
        pass

def add_coordinates(coord_str: str) -> Tuple[float, float]:
    coords = coord_str.split(':')
    x, y = coords[0], coords[1]
    return float(x), float(y)

class MetricManager:
    def __init__(self, vectorized_metrics: List[VectorizedMetric], supplementary_df: pd.DataFrame, groups: List[str]):
        self.vectorized_metrics = vectorized_metrics
        self.supplementary_df = supplementary_df
        self.groups = groups
        self.grouped_data = self._precompute_grouped_data(supplementary_df, groups)

    @staticmethod
    def _precompute_grouped_data(supplementary_df, groups):
        grouped_data = {}
        grouped = supplementary_df.groupby(groups)
        for key, group in grouped:
            grouped_data[key] = group
        return grouped_data

    def process_row(self, row: pd.Series):
        # Create temporary DataFrame for vectorized metrics
        group_key = tuple(row[group] for group in self.groups)

        # Get the precomputed group DataFrame
        temp_df = self.grouped_data.get(group_key, pd.DataFrame(columns=self.supplementary_df.columns))
        # Process 'hit_coordinates' if the column exists
        if 'hit_coordinates' in temp_df.columns:
            temp_df['hit_coordinates'] = temp_df['hit_coordinates'].map(
                lambda x: (np.nan, np.nan) if pd.isna(x) else add_coordinates(x)
            )
        # Process vectorized metrics
        results = {}
        for metric in self.vectorized_metrics:
            if metric.requires_row:
                metric.add_row(row)
            results.update(metric.calculate(temp_df))
        return results

    def apply_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.vectorized_metrics:
            return df
        metrics_df = df.apply(self.process_row, axis=1)
        return pd.concat([df, pd.DataFrame(metrics_df.tolist())], axis=1)

    async def async_apply_metrics(self, df: pd.DataFrame, max_concurrent: int = 10) -> pd.DataFrame:
        if not self.vectorized_metrics:
            return df

        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_row_async(row):
            async with semaphore:
                return await asyncio.to_thread(self.process_row, row)

        tasks = [process_row_async(row) for _, row in df.iterrows()]
        results = await asyncio.gather(*tasks)

        metrics_df = pd.DataFrame(results)
        return pd.concat([df, metrics_df], axis=1)


class DBMetric:
    def __init__(self, data: Dict):
        self.metric_name = data.get('metric_name')
        self.sql_value = data.get('sql_value', None)
        self.is_all_plays = data.get('is_all_plays', 0)
        self.is_totals_batter = data.get('is_totals_batter', 0)
        self.is_totals_pitcher = data.get('is_totals_pitcher', 0)
        self.is_totals_fielder = data.get('is_totals_fielder', 0)
        self.is_grouping = data.get('is_grouping', 0)
        self.metric_description = data.get('metric_description')
        self.hidden = data.get('hidden', 0)
        self.dependencies = []
        dependencies = data.get('dependencies', '')
        if dependencies:
            self.dependencies = dependencies.split(',')

    def __repr__(self):
        return (
            f"Metric(metric_name='{self.metric_name}', sql_value='{self.sql_value}', "
            f"is_all_plays={self.is_all_plays}, is_totals_batter={self.is_totals_batter}, "
            f"is_totals_pitcher={self.is_totals_pitcher}, is_totals_fielder={self.is_totals_fielder}, "
            f"is_grouping={self.is_grouping}, metric_description='{self.metric_description}', "
            f"hidden={self.hidden}, dependencies='{self.dependencies}')"
        )