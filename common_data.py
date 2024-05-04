import numpy as np
from typing import *


def calculate_contacts(pitch_results: List[str], zones: List[str]) -> Tuple[float, float, float]:
    zones = [int(x) for x in zones]
    out_of_zone = 0
    in_zone_contact = 0
    in_zone = 0
    chase = 0
    contact = 0
    total_swings = 0
    for i, d in enumerate(pitch_results):
        if zones[i] > 9:
            out_of_zone += 1
        if d == 'Foul' or 'In play' in d:
            contact += 1
            total_swings += 1
            if 0 < zones[i] < 9:
                in_zone_contact += 1
                in_zone += 1
        if d == 'Foul Tip' or 'swinging' in d.lower():
            total_swings += 1
            if 0 < zones[i] < 9:
                in_zone += 1
            elif zones[i] > 9:
                chase += 1
    contact_percent = contact / total_swings if total_swings else 0
    zone_contact = in_zone_contact / in_zone if in_zone else 0
    chase_percent = chase / out_of_zone if out_of_zone else 0
    return {'contact_percent': contact_percent,
            'zone_contact': zone_contact,
            'chase_percent': chase_percent}


def add_percentile(row: Union[List, Dict]) -> Union[List, Dict]:
    if isinstance(row, dict):
        hit_speeds = row.get('percentile_90', '')
        if hit_speeds is None:
            row['percentile_90'] = 0
            return row
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
    strike_ratio = round(strikes / len(pitch_results), 4)
    csw = round(strikes + swinging_strikes / len(pitch_results), 4)
    swstr = round(swinging_strikes / len(pitch_results), 4)
    ball_ratio = round(balls / len(pitch_results), 4)
    return {'o_strike_percent': strike_ratio * 100,
            'o_csw': csw * 100,
            'o_swstr': swstr * 100,
            'o_ball_percent': ball_ratio * 100}
