from typing import *
import pandas as pd
import numpy as np
import math
import asyncio
from .queries import BaseQueryBuilder, PlaysBuilder
from .complex_metrics import COMPLEX_METRICS_DICT, ExpectedWeightedOBA
from .abc import BaseQueryFactory, BaseDBManager

batter_default_metrics = ('name', 'league', 'pitches', 'bip', 'percentile_90','launch_angles', 'avg_ev', 'max_ev',
                          'avg_hit_angle', 'barrel_per_bbe', 'contact_percent')

pitcher_default_metrics = ('name', 'league', 'count', 'batters_faced', 'pitches_thrown','strike_outs', 'walks',
                           'k_min_bb')


def add_coordinates(coord_str: str) -> Tuple[float, float]:
    coords = coord_str.split(':')
    x, y = coords[0], coords[1]
    return float(x), float(y)


class Processor:
    def __init__(self, query_builder: BaseQueryBuilder, query_factory: BaseQueryFactory, max_concurrent: int = 10):
        self.query_builder = query_builder
        self.query_factory = query_factory
        self.db_manager: BaseDBManager = query_factory.db_manager
        self.max_concurrent = max_concurrent
        self.metric_instances = []
        self.semaphore = asyncio.Semaphore(self.max_concurrent)

    async def _build_temp_df(self, row) -> pd.DataFrame:
        python_metrics = self.query_builder.python_metrics
        builder: PlaysBuilder = await self.query_factory.create_query(metrics=python_metrics,
                                                                      player_type=self.query_builder.player_type,
                                                                      builder_cls=PlaysBuilder)
        for group_column in self.query_builder.get_group_columns():
            value = row[group_column]
            if group_column == 'player_id':
                builder.add_dynamic_where(builder.player_type + '_id', value)
            elif group_column == 'team_name':
                builder.add_team(value)
            elif group_column == 'name':
                builder.add_name(value)
            else:
                builder.add_dynamic_where(group_column, value)
        for where, arg in zip(self.query_builder.get_where_clauses(), self.query_builder.get_args()):
            if 'name ' in where:
                continue
            builder.add_raw_where(where)
            builder.args.append(arg)
        data = await self.db_manager.fetch_all(builder.get_query(), builder.get_args())
        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data)

    async def process_row(self, index, row: pd.Series):
        async with self.semaphore:
            temp_df = await self._build_temp_df(row)
            if temp_df.empty:
                return index, {col: None for col in self.query_builder.python_metrics}

            temp_df = temp_df.map(lambda x: np.nan if x is None else x)
            if 'hit_coordinates' in temp_df.columns:
                temp_df['hit_coordinates'] = temp_df['hit_coordinates'].map(
                    lambda x: (np.nan, np.nan) if pd.isna(x) else add_coordinates(x)
                )
            # Process vectorized metrics
            results = {}
            for metric in self.metric_instances:
                if metric.requires_row:
                    metric.add_row(row)
                try:
                    results.update(metric.calculate(temp_df))
                except Exception as e:
                    print(temp_df.to_dict())
                    print(temp_df)
                    raise e
            return index, results

    async def apply_per_row(self, df: pd.DataFrame) -> pd.DataFrame:

        tasks = [self.process_row(index, row) for index, row in df.iterrows()]
        results_list = await asyncio.gather(*tasks)

        # Convert results into a DataFrame with explicit indexing
        results_df = pd.DataFrame([result[1] for result in results_list], index=[result[0] for result in results_list])

        # Merge results back into the original DataFrame
        return df.join(results_df)

    async def create_and_calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        python_metrics = self.query_builder.python_metrics
        if not python_metrics:
            final_df = df
        else:
            metric_classes = [COMPLEX_METRICS_DICT[m] for m in python_metrics if m in COMPLEX_METRICS_DICT.keys()]
            metric_classes = list(set(metric_classes))
            metric_instances = await self.async_initialize_metric_classes(metric_classes)
            self.metric_instances = metric_instances
            final_df = await self.apply_per_row(df)
        final_df = final_df[self.query_builder.get_metric_names() + python_metrics]
        return final_df

    async def calculate_batter_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        python_metrics = self.query_builder.python_metrics
        if 'OBS' in python_metrics:
            df['OBS'] = df['OBP'] + df['SLG']
        return await self.create_and_calculate_metrics(df)

    async def calculate_pitcher_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        return await self.create_and_calculate_metrics(df)

    async def async_initialize_metric_classes(self, metric_classes):
        metric_instances = []
        for metric_class in metric_classes:
            if metric_class == ExpectedWeightedOBA:
                batted_ball_probs = await self.db_manager.fetch_all('SELECT * FROM batted_ball_probabilities')
                metric_instances.append(metric_class(batted_ball_probs))
            else:
                metric_instances.append(metric_class())
        return metric_instances

def calc_release_pos(data) -> tuple[float, float]:
    # going to need to make this work with null values
    x0 = data['x0']
    y0 = data['y0']
    z0 = data['z0']
    ax = data['ax']
    ay = data['ay']
    az = data['az']
    vx0 = data['vx0']
    vy0 = data['vy0']
    vz0 = data['vz0']
    extension = data['extension']
    desired_y = 60 - extension

    a_y = 0.5 * ay
    b_y = vy0
    c_y = y0 - desired_y

    discriminant_y = b_y ** 2 - 4 * a_y * c_y
    t_y1 = (-b_y + math.sqrt(discriminant_y)) / (2 * a_y)
    t_y2 = (-b_y - math.sqrt(discriminant_y)) / (2 * a_y)

    t_y = min(t_y1, t_y2)

    x_t = x0 + vx0 * t_y + 0.5 * ax * t_y ** 2
    z_t = z0 + vz0 * t_y + 0.5 * az * t_y ** 2

    return x_t, z_t