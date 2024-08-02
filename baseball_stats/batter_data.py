from typing import *
import pandas as pd
import numpy as np
from .utils import PlaysBuilder, TotalsBuilder
from .common_data import insert_league_averages, get_combined_data, is_contact, is_swing, is_barreled

default_metrics = ('batter_name', 'league', 'pitches', 'zones', 'pitch_results', 'bip', 'percentile_90',
                   'launch_angles', 'avg_ev', 'max_ev', 'avg_hit_angle', 'barrel_per_bbe')


def add_batter_league_averages(league: str):
    keys = ['percentile_90', 'avg_ev', 'max_ev', 'avg_hit_angle', 'contact_percent', 'zone_contact',
            'chase_percent', 'swing_percent', 'zone_swing_percent']
    data = get_batter_data(None, metrics=keys, league=league)
    insert_league_averages(league, data.to_dict(orient='records'), keys)


def get_batter_data(name: str | List[str] = None, metrics: List[str] = default_metrics, league: str | List[str] = None,
                    game_type: str = 'R', dates: Tuple[str, str] = None, year: str = '2024') -> pd.DataFrame:
    builder1 = PlaysBuilder(metrics, 'batter_name')
    builder2 = TotalsBuilder(metrics, 'hitters')
    for b in (builder1, builder2):
        b.add_name(name)
        b.add_all_but_name(league, dates, year, game_type)
        b.finish_query()
    rows = get_combined_data(builder1, builder2)
    if rows.empty:
        return rows
    processed_rows = process_batter_rows(rows, metrics)
    return processed_rows


def process_batter_rows(df: pd.DataFrame, metrics: List[str]) -> pd.DataFrame:
    metric_functions = {
        'barrel_per_bbe': calculate_barrel,
        'percentile_90': calculate_percentile,
        'contact_percent': calculate_contacts,
        'zone_contact': calculate_contacts,
        'chase_percent': calculate_contacts,
        'swing_percent': calculate_contacts,
        'zone_swing_percent': calculate_contacts
    }

    result_dfs = [df]

    used_contact = False
    for metric in metrics:
        if metric in metric_functions:
            if metric_functions[metric] == calculate_contacts:
                if not used_contact:
                    result_dfs.append(metric_functions[metric](df.copy()))
                    used_contact = True
            else:
                result_dfs.append(metric_functions[metric](df.copy()))

    final_df = pd.concat(result_dfs, axis=1)
    final_df = final_df.drop(['hit_speeds', 'zones', 'pitch_results', 'launch_angles'], errors='ignore', axis=1)
    return final_df


def calculate_barrel_percent(angles: List[str], speeds: List[str]) -> float:
    angles = [float(x) for x in angles]
    speeds = [float(x) for x in speeds]
    count_barreled = sum(is_barreled(launch_angle, exit_velocity) for launch_angle, exit_velocity in zip(angles, speeds))
    return (count_barreled / len(angles)) * 100 if angles else 0


def calculate_barrel(df: pd.DataFrame) -> pd.DataFrame:
    df['hit_speeds'] = df['hit_speeds'].str.split(',')
    df['launch_angles_list'] = df['launch_angles'].str.split(',')
    df['barrel_per_bbe'] = df.apply(lambda row: calculate_barrel_percent(row['launch_angles_list'], row['hit_speeds']), axis=1)
    return df['barrel_per_bbe']


def calculate_percentile(df: pd.DataFrame) -> pd.DataFrame:
    df['percentile_90'] = df['hit_speeds'].apply(lambda x: np.percentile([float(i) for i in x.split(',') if i], 90) if x else 0)
    return df['percentile_90']


def calculate_contacts(df: pd.DataFrame) -> pd.DataFrame:
    df['descriptions'] = df['pitch_results'].str.split(',')
    df['zones_list'] = df['zones'].str.split(',')
    contact_percents = df.apply(lambda row: calculate_contacts_metrics(row['descriptions'], row['zones_list']), axis=1)
    contact_df = pd.DataFrame(contact_percents.tolist(), index=df.index)
    return contact_df


def calculate_contacts_metrics(pitch_results: List[str], zones: List[str]) -> Dict[str, float]:
    zones = [int(x) if x else 0 for x in zones]
    if len(zones) < len(pitch_results):
        zones.extend([0] * (len(pitch_results) - len(zones)))

    out_of_zone = in_zone_contact = in_zone = in_zone_swing = chase = contact = total_swings = 0
    for i, d in enumerate(pitch_results):
        if 0 < zones[i] < 9:
            in_zone += 1
        elif zones[i] > 9:
            out_of_zone += 1
        if is_swing(d):
            total_swings += 1
            if 0 < zones[i] < 9:
                in_zone_swing += 1
            elif zones[i] > 9:
                chase += 1
        if is_contact(d):
            contact += 1
            if 0 < zones[i] < 9:
                in_zone_contact += 1

    contact_percent = contact / total_swings if total_swings else 0
    zone_contact = in_zone_contact / in_zone_swing if in_zone_swing else 0
    chase_percent = chase / out_of_zone if out_of_zone else 0
    swing_percent = total_swings / len(pitch_results) if pitch_results else 0
    zone_swing_percent = in_zone_swing / in_zone if in_zone else 0

    return {
        'contact_percent': round(contact_percent, 4),
        'zone_contact': round(zone_contact, 4),
        'chase_percent': round(chase_percent, 4),
        'swing_percent': round(swing_percent, 4),
        'zone_swing_percent': round(zone_swing_percent, 4)
    }
