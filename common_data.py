import numpy as np
from utils import connect
from typing import *

all_swings = ['Foul', 'Foul Bunt', 'Foul Tip Bunt', 'Foul Pitchout', 'Missed Bunt', 'Foul Tip',
              'Swinging Strike', 'Swinging Strike (Blocked)', 'Swinging Pitchout']


def insert_league_averages(league, processed_data, keys):
    arrays = {}
    for row in processed_data:
        for key in keys:
            arrays.setdefault(key, [])
            arrays[key].append(row[key])
    args = []
    insert_query = '''
                INSERT INTO league_averages (league, metric, mean, stddev) VALUES (?, ?, ?, ?) 
                ON CONFLICT(league, metric) DO UPDATE SET
                mean = excluded.mean,
                stddev = excluded.stddev
        '''
    for k, value in arrays.items():
        value = [v for v in value if v is not None]
        if len(value) == 0:
            std_dev = 0
            avg = 0
        else:
            std_dev = np.std(value)
            avg = sum(value) / len(value)
        args.append((league, k, avg, std_dev))
    conn, cursor = connect()
    cursor.executemany(insert_query, args)
    conn.commit()
    conn.close()


def is_barreled(launch_angle, exit_velocity):
    if exit_velocity < 98:
        return False
    elif exit_velocity == 98:
        return 26 <= launch_angle <= 30
    else:
        # Calculate expanded range
        if exit_velocity <= 116:
            additional_degrees = 3 + 2 * (exit_velocity - 100) if exit_velocity > 100 else 1 * (exit_velocity - 99)
            min_angle = 26 - additional_degrees
            max_angle = 30 + additional_degrees
        else:
            min_angle = 8
            max_angle = 50

        return min_angle <= launch_angle <= max_angle


def calculate_barrel_percent(angles: List, speeds: List):
    angles = [float(x) for x in angles]
    speeds = [float(x) for x in speeds]
    count_barreled = 0
    for launch_angle, exit_velocity in zip(angles, speeds):
        if is_barreled(launch_angle, exit_velocity):
            count_barreled += 1
    return (count_barreled / len(angles)) * 100 if angles else 0


def is_contact(pitch_r):
    return 'play' in pitch_r or pitch_r == 'Foul' or pitch_r == 'Foul Bunt'


def is_swing(pitch_r):
    return pitch_r in all_swings or 'play' in pitch_r


def calculate_contacts(pitch_results: List[str], zones: List[str]) -> Dict:
    zones = [int(x) if x is not None else 0 for x in zones]
    if len(zones) < len(pitch_results):
        zones = [0] * len(pitch_results)
    out_of_zone = 0
    in_zone_contact = 0
    in_zone = 0
    in_zone_swing = 0
    chase = 0
    contact = 0
    total_swings = 0
    for i, d in enumerate(pitch_results):
        if 0 < zones[i] < 9:
            in_zone += 1  # counts all in zone pitches
        elif zones[i] > 9:
            out_of_zone += 1  # counts all out of zone pitches
        if is_swing(d):
            total_swings += 1
            if 0 < zones[i] < 9:
                in_zone_swing += 1
            elif zones[i] > 9:
                chase += 1  # counts out of zone swings
        if is_contact(d):
            contact += 1
            if 0 < zones[i] < 9:
                in_zone_contact += 1

    contact_percent = contact / total_swings if total_swings else 0
    zone_contact = in_zone_contact / in_zone_swing if in_zone_swing else 0
    chase_percent = chase / out_of_zone if out_of_zone else 0
    swing_percent = total_swings / len(pitch_results) if len(pitch_results) else 0
    zone_swing_percent = in_zone_swing / in_zone if in_zone else 0
    return {'contact_percent': round(contact_percent, 4),
            'zone_contact': round(zone_contact, 4),
            'chase_percent': round(chase_percent, 4),
            'swing_percent': round(swing_percent, 4),
            'zone_swing_percent': round(zone_swing_percent, 4)}


def add_percentile(row: Union[List, Dict]) -> Union[List, Dict]:
    if isinstance(row, dict):
        hit_speeds = row.get('percentile_90', None)
        if hit_speeds is None:
            row['percentile_90'] = 0
            return row
        else:
            hit_speeds = hit_speeds.split(',')
    else:
        hit_speeds = row[-1].split(',')
    hit_speeds = [float(speed) for speed in hit_speeds if speed is not None]

    percentile_90 = np.percentile(hit_speeds, 90) if hit_speeds else 0

    if isinstance(row, dict):
        row['percentile_90'] = percentile_90
        return row
    else:
        row[-1] = percentile_90
        return row


def calculate_percents(pitch_results: List[str]) -> Tuple[float, float, float]:
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
    return {'o_strike_percent': strike_ratio * 100,
            'o_csw_percent': csw * 100,
            'o_swstr_percent': swstr * 100,
            'o_ball_percent': ball_ratio * 100}
