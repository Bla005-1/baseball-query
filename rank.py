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


def calculate_player_score(row: pd.Series) -> float:
    score = 0.0
    for metric, weight in weights.items():
        value = row.get(metric, 0)
        if value is None:
            value = 0
        if metric == 'chase_percent':
            value = 100 - value  # Invert chase_percent
        score += value * weight
    return score


def rank_players(data: pd.DataFrame) -> pd.DataFrame:
    data['score'] = data.apply(calculate_player_score, axis=1)
    return data.sort_values('score', ascending=False)


# Example usage
def main():
    league = 'MLB'
    names = select_data('SELECT DISTINCT name FROM pitchers WHERE game_type = ? AND league = ?', ('R', league))
    names = [r['name'] for r in names]
    batter_data = pd.DataFrame(get_pitcher_data(names, league=league))
    extra_batter_data = pd.DataFrame(basic_pitch_calcs(names, league=league))
    merged_data = pd.merge(batter_data, extra_batter_data, left_on='name', right_on='name')
    ranked_players = rank_players(merged_data)
    for count, (index, row) in enumerate(ranked_players.head(30).iterrows(), start=1):
        print(f"{count}. {row['name']}, Score: {row['score']}")


if __name__ == "__main__":
    main()
