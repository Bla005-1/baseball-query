from typing import *
import pandas as pd
import math
from .complex_metrics import COMPLEX_METRICS_DICT
from .metrics_abc import MetricManager
from .db_tools import select_data
from .queries import QueryBuilder
from .errors import *


batter_default_metrics = ('name', 'league', 'pitches', 'zones', 'pitch_results', 'bip', 'percentile_90',
                   'launch_angles', 'avg_ev', 'max_ev', 'avg_hit_angle', 'barrel_per_bbe', 'contact_percent')

pitcher_default_metrics = ('name', 'league', 'count', 'pitch_results', 'batters_faced', 'pitches_thrown',
                   'strike_outs', 'walks', 'k_min_bb')


def process_batter_rows(df: pd.DataFrame, metrics: List[str], groups: List[str],
                        supplementary_df: pd.DataFrame) -> pd.DataFrame:
    if 'OBS' in metrics:
        df['OBS'] = df['OBP'] + df['SLG']
    metric_classes = [COMPLEX_METRICS_DICT[m] for m in metrics if m in COMPLEX_METRICS_DICT.keys()]
    metric_classes = list(set(metric_classes))
    metric_classes = [m() for m in metric_classes]
    manager = MetricManager(metric_classes, supplementary_df, groups)
    final_df = manager.apply_metrics(df)
    final_df = final_df[metrics]

    return final_df


def process_pitcher_rows(df: pd.DataFrame, metrics: List[str], groups: List[str],
                        supplementary_df: pd.DataFrame) -> pd.DataFrame:
    metric_classes = [COMPLEX_METRICS_DICT[m]() for m in metrics if m in COMPLEX_METRICS_DICT.keys()]
    manager = MetricManager(metric_classes, supplementary_df, groups)
    final_df = manager.apply_metrics(df)
    final_df = final_df[metrics]

    return final_df

def get_combined_data(query1: QueryBuilder, query2: QueryBuilder, merge_on: Iterable[str]) -> pd.DataFrame:
    if query1 and query2:
        data1 = select_data(query1.get_query(), query1.get_args())
        data2 = select_data(query2.get_query(), query2.get_args())
        if len(data1) == 0 or len(data2) == 0:
            raise NoDataFoundError(query1=query1, query2=query2)
        df = pd.merge(pd.DataFrame(data1), pd.DataFrame(data2), on=merge_on)
    elif query1:
        data1 = select_data(query1.get_query(), query1.get_args())
        df = pd.DataFrame(data1)
    elif query2:
        data2 = select_data(query2.get_query(), query2.get_args())
        df = pd.DataFrame(data2)
    else:
        raise EmptyQueryError()
    if df.empty:
        raise NoDataFoundError(query1=query1, query2=query2)
    return df

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