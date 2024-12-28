from abc import ABC, abstractmethod
from typing import List, Tuple, Dict
import pandas as pd
import numpy as np
import time

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

    def process_row(self, row: pd.Series):
        start = time.time()
        # Create temporary DataFrame for vectorized metrics
        mask = np.logical_and.reduce(
            [self.supplementary_df[group] == row[group] for group in self.groups]
        )
        temp_df = self.supplementary_df[mask]

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
        '''
        # Process iteration-based metrics
        for metric in self.iteration_metrics:
            metric.reset()

        depend_groups: List[Tuple] = list(set(metric.dependencies for metric in self.iteration_metrics))
        for dependencies in depend_groups:
            if len(dependencies) == 1:
                # Single dependency: Extract column as a Series
                group = temp_df[dependencies[0]]
                iterator = enumerate(group)
            else:
                # Multiple dependencies: Extract relevant columns and zip them
                group = temp_df[dependencies]
                iterator = enumerate(group.itertuples(index=False, name=None))  # Create row-wise tuples
            # Process each item in the group
            for i, values in iterator:
                for metric in self.iteration_metrics:
                    # Ensure the metric depends on the current dependencies
                    if all(dep in metric.dependencies for dep in dependencies):
                        metric.update(i, values)  # Pass single value for single dependency
        for metric in self.iteration_metrics:
            results.update(metric.finalize())
        '''
        return results

    def apply_metrics(self, df):
        if not self.vectorized_metrics:
            return df
        metrics_df = df.apply(self.process_row, axis=1)
        return pd.concat([df, pd.DataFrame(metrics_df.tolist())], axis=1)


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
        dependencies = data.get('dependencies', '')
        if dependencies:
            self.dependencies = dependencies.split(',')
        else:
            self.dependencies = []

    def __repr__(self):
        return (
            f"Metric(metric_name='{self.metric_name}', sql_value='{self.sql_value}', "
            f"is_all_plays={self.is_all_plays}, is_totals_batter={self.is_totals_batter}, "
            f"is_totals_pitcher={self.is_totals_pitcher}, is_totals_fielder={self.is_totals_fielder}, "
            f"is_grouping={self.is_grouping}, metric_description='{self.metric_description}', "
            f"hidden={self.hidden}, dependencies='{self.dependencies}')"
        )