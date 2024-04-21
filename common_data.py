import numpy as np
from typing import *


def calculate_contacts(descriptions: List[str], zones: List[str]) -> Tuple[float, float, float]:
    out_of_zone = 0
    in_zone_contact = 0
    in_zone = 0
    chase = 0
    contact = 0
    total_swings = 0
    for i, d in enumerate(descriptions):
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
    return contact_percent, zone_contact, chase_percent


def add_percentile(row: Union[List, Dict]) -> Union[List, Dict]:
    if isinstance(row, dict):
        hit_speeds = row.get('percentile_90', '').split(',')
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
