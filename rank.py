import json
import pandas as pd
from typing import List, Dict
from utils import select_data
from pitch_data import get_pitcher_data, basic_pitch_calcs

with open('pitcher_optimized_weights.json', 'r') as f:
    weights = json.load(f)

if not weights:
    print('Using defaults')
    weights = {
        'avg_ev': 0.19,
        'max_ev': 0.06,
        'barrel_per_bbe': 0.19,
        'contact_percent': 0.14,
        'zone_contact': 0.09,
        'chase_percent': 0.04,
        'swing_percent': 0.04,
        'zone_swing_percent': 0.07,
        'avg_hit_angle': 0.04,
        'percentile_90': 0.14
    }


def calculate_player_score(row: Dict) -> float:
    score = 0.0
    for metric, weight in weights.items():
        value = row.get(metric, 0)
        if value is None:
            value = 0
        if metric == 'chase_percent':
            value = 100 - value  # Invert chase_percent
        score += value * weight
    return score


def rank_players(data: List[Dict]) -> List[Dict]:
    for row in data:
        row['score'] = calculate_player_score(row)
    return sorted(data, key=lambda x: x['score'], reverse=True)


# Example usage
def main():
    league = 'MLB'
    names = select_data('SELECT DISTINCT name FROM pitchers WHERE game_type = ? AND league = ?', ('R', league))
    names = [r['name'] for r in names]
    batter_data = pd.DataFrame(get_pitcher_data(names, league=league))
    extra_batter_data = pd.DataFrame(basic_pitch_calcs(names, league=league))
    for batter in batter_data:
        if batter['IP'] < 3:
            batter_data.remove(batter)
    merged_data = pd.merge(batter_data, extra_batter_data, left_on='pitcher_name', right_on='name')
    ranked_players = rank_players(merged_data)
    count = 1
    for player in ranked_players[:30]:
        print(f"{count}. {player['pitcher_name']}, Score: {player['score']}")
        count += 1


if __name__ == "__main__":
    main()
