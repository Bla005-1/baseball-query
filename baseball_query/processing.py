from typing import *
import pandas as pd
import math
from .complex_metrics import COMPLEX_METRICS_DICT, ExpectedWeightedOBA
from .db_access_layer import DBManager
from .metrics_abc import MetricManager


batter_default_metrics = ('name', 'league', 'pitches', 'bip', 'percentile_90','launch_angles', 'avg_ev', 'max_ev',
                          'avg_hit_angle', 'barrel_per_bbe', 'contact_percent')

pitcher_default_metrics = ('name', 'league', 'count', 'batters_faced', 'pitches_thrown','strike_outs', 'walks',
                           'k_min_bb')


class Processor:
    def __init__(self, db_manager: DBManager):
        self.db_manager = db_manager

    async def calculate_batter_rows(self, df: pd.DataFrame, metrics: List[str], groups: List[str],
                            supplementary_df: pd.DataFrame) -> pd.DataFrame:
        if 'OBS' in metrics:
            df['OBS'] = df['OBP'] + df['SLG']
        if supplementary_df.empty:
            final_df = df
        else:
            metric_classes = [COMPLEX_METRICS_DICT[m] for m in metrics if m in COMPLEX_METRICS_DICT.keys()]
            metric_classes = list(set(metric_classes))
            metric_instances = await self.async_initialize_metric_classes(metric_classes)
            manager = MetricManager(metric_instances, supplementary_df, groups)
            final_df = await manager.async_apply_metrics(df)
        final_df = final_df[metrics]
        return final_df

    async def calculate_pitcher_rows(self, df: pd.DataFrame, metrics: List[str], groups: List[str],
                            supplementary_df: pd.DataFrame) -> pd.DataFrame:
        if supplementary_df.empty:
            final_df = df
        else:
            metric_classes = [COMPLEX_METRICS_DICT[m]() for m in metrics if m in COMPLEX_METRICS_DICT.keys()]
            metric_instances = await self.async_initialize_metric_classes(metric_classes)
            manager = MetricManager(metric_instances, supplementary_df, groups)
            final_df = await manager.async_apply_metrics(df)
        final_df = final_df[metrics]
        return final_df

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