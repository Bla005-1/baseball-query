import math
import pandas as pd
from typing import *
from .queries import TotalsBuilder, PlaysBuilder
from .common_data import insert_league_averages, get_combined_data
from .static_data import requires_pitch_results


default_metrics = ('pitcher_name', 'name', 'league', 'count', 'pitch_results', 'batters_faced', 'pitches_thrown',
                   'strike_outs', 'walks', 'k_bb')


def add_pitcher_league_averages(league: str):
    data = get_pitcher_data(None, ['pitch_results', 'pitcher_name'], league=league, game_type='R', year='2024')
    keys = ['strike_percent', 'csw_percent', 'swstr_percent', 'ball_percent']
    insert_league_averages(league, data.to_dict(orient='records'), keys)


def get_pitcher_data(name: str | List[str] = None, metrics: List[str] = default_metrics, league: str | List[str] = None,
                     game_type: str = 'R', dates: Tuple[str, str] = None, year: str = '2024') -> pd.DataFrame:
    builder1 = PlaysBuilder(metrics)
    builder2 = TotalsBuilder(metrics, 'pitchers')
    for b in (builder1, builder2):
        b.add_name(name)
        b.add_all_but_name(league, dates, year, game_type)
        b.finish_query()
    rows = get_combined_data(builder1, builder2)
    if rows.empty:
        return rows
    processed_rows = process_pitcher_rows(rows, metrics)
    return processed_rows


def process_pitcher_rows(df: pd.DataFrame, metrics: List[str]) -> pd.DataFrame:
    if any(m in requires_pitch_results for m in set(metrics)):
        pitch_results_split = df['pitch_results'].str.split(',')
        percents = pitch_results_split.apply(pitcher_per_pitch_calcs)
        percents_df = pd.DataFrame(percents.tolist())
        combined_data = pd.concat([df.drop(columns=['pitch_results']), percents_df], axis=1)
        return combined_data
    return df


def pitcher_per_pitch_calcs(pitch_results: List[str]):
    strikes = 0
    balls = 0
    swinging_strikes = 0
    for i, d in enumerate(pitch_results):
        if d == 'Foul Tip' or 'swinging' in d.lower():
            swinging_strikes += 1
        elif 'strike' in d.lower():
            strikes += 1
        elif 'ball' in d.lower() or 'hit by' in d.lower() or d == 'Pitchout':
            balls += 1
    strike_ratio = (len(pitch_results) - balls) / len(pitch_results)
    csw = (strikes + swinging_strikes) / len(pitch_results)
    swstr = swinging_strikes / len(pitch_results)
    ball_ratio = balls / len(pitch_results)
    return {'strike_percent': strike_ratio * 100,
            'csw_percent': csw * 100,
            'swstr_percent': swstr * 100,
            'ball_percent': ball_ratio * 100}


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
