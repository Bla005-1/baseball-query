import json
import pandas as pd
import sys
from typing import Dict
from utils import select_data
from pitch_data import get_pitcher_data, basic_pitch_calcs
from batter_data import get_batter_data, basic_batt_calcs

with open('batter_optimized_weights.json', 'r') as f:
    batter_weights: Dict = json.load(f)

with open('pitcher_optimized_weights.json', 'r') as f:
    pitcher_weights: Dict = json.load(f)


def calculate_player_score(row: pd.Series, weights: Dict) -> float:
    score = 0.0
    for metric, weight in weights.items():
        value = row.get(metric, 0)
        if value is None:
            value = 0
        if metric == 'chase_percent':
            value = 100 - value  # Invert chase_percent
        score += value * weight
    return score


def rank_players(data: pd.DataFrame, player) -> pd.DataFrame:
    if player == 'pitchers':
        weights = pitcher_weights
    else:
        weights = batter_weights
    data['score'] = data.apply(lambda row: calculate_player_score(row, weights), axis=1)
    return data.sort_values('score', ascending=False)


# Example usage
def main(league):
    for get_func, extra_func, string in [(get_batter_data, basic_batt_calcs, 'hitters'),
                                         (get_pitcher_data, basic_pitch_calcs, 'pitchers')]:
        names = select_data(f'SELECT DISTINCT name FROM {string} WHERE game_type = ? AND league = ?', ('R', league))
        names = [r['name'] for r in names]
        data = pd.DataFrame(get_func(names, league=league))
        extra_data = pd.DataFrame(extra_func(names, league=league))
        try:
            merged_data = pd.merge(data, extra_data, left_on='name', right_on='name')
        except KeyError:
            merged_data = pd.merge(data, extra_data, left_on='batter_name', right_on='name')
        ranked_players = rank_players(merged_data, string)
        rank_str = ''
        for count, (index, row) in enumerate(ranked_players.head(100).iterrows(), start=1):
            rank_str += f"{count}: {row['name']}: Score={row['score']}\n"
            print(rank_str[:-2])
        with open(f'{string}_top_100_{league}.txt', 'w') as fp:
            fp.write(rank_str)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        the_league = sys.argv[1]
    else:
        the_league = 'MLB'
    main(the_league)
