import math
from typing import *
from utils import select_data, QueryBuilder
from common_data import calculate_percents, insert_league_averages


def add_pitcher_league_averages(league: str):
    pitch_query = '''
            SELECT league, 
                GROUP_CONCAT(pitch_result) AS pitch_results
            FROM all_plays WHERE league = ? AND game_type = 'R' AND date LIKE "2024%"
            GROUP BY pitcher_name
            '''
    data = select_data(pitch_query, (league,))
    processed_data = process_pitch_rows(data)
    keys = ['strike_percent', 'csw_percent', 'swstr_percent', 'ball_percent']
    insert_league_averages(league, processed_data, keys)


def get_overall_stats(query1: str, query2: str, args: Iterable) -> List:
    overall_pitchers = select_data(query1, args)
    more_overall_pitchers = select_data(query2, args)
    combined_overall = {}
    for p in overall_pitchers:
        a = combined_overall.setdefault(p['league'] + p['pitcher_name'], {})
        a.update(calculate_percents(p.get('pitch_results', '').split(',')))
    for mp in more_overall_pitchers:
        a = combined_overall.setdefault(mp['league'] + mp['name'], {})
        a.update(mp)
    return list(combined_overall.values())


def process_pitch_rows(pitch_data: List[Dict]) -> List[Dict]:
    processed_data = []
    for pitch_row in pitch_data:
        my_data = {}
        descriptions = pitch_row.get('pitch_results', '').split(',')
        percents = calculate_percents(descriptions)
        pitch_row['strike_percent'] = percents['o_strike_percent']
        pitch_row['csw_percent'] = percents['o_csw_percent']
        pitch_row['swstr_percent'] = percents['o_swstr_percent']
        pitch_row['ball_percent'] = percents['o_ball_percent']
        pitch_row.pop('pitch_results')
        my_data.update(pitch_row)
        processed_data.append(my_data)
    return processed_data


def basic_pitch_calcs(name: str | List[str], league: str = None, dates: Tuple[str, str] = None,
                      game_type: str = None) -> List[Dict] | Dict:
    query = '''
        SELECT name,
            SUM(innings_pitched) AS IP,
            9 * SUM(earned_runs) / SUM(innings_pitched) AS ERA,
            SUM(strike_outs) / SUM(CAST(batters_faced AS REAL)) AS strikeout_ratio,
            SUM(base_on_balls) / SUM(CAST(batters_faced AS REAL)) AS walk_ratio,
            SUM(fly_outs) AS fly_outs,
            SUM(ground_outs) AS ground_outs,
            SUM(air_outs) AS air_outs,
            SUM(runs) AS runs,
            SUM(doubles) AS doubles,
            SUM(triples) AS triples,
            SUM(home_runs) AS home_runs,
            SUM(at_bats) AS at_bats,
            SUM(balls) AS balls,
            SUM(strikes) AS strikes,
            GROUP_CONCAT(pitches_thrown) AS cumulative_pitches
        FROM pitchers
    '''
    builder = QueryBuilder(query)
    builder.add_name(name, 'name')
    builder.add_all_but_name(league, dates, game_type=game_type)
    builder.finish_query()
    calcs = select_data(builder.get_query(), builder.get_args())
    for i, c in enumerate(calcs):
        calcs[i] = {k: round(v or 0, 3) if not isinstance(v, str) else v for k, v in c.items()}
    if len(calcs) == 1:
        return calcs[0]
    return calcs


# could make get_data a common function between pitch and batt since only query is different
def get_pitcher_data(name: str | List[str], league: str = None, game_type: str = 'R',
                     dates: Tuple[str, str] = None, year: str = '2024') -> List[Dict] | Dict:
    pitch_query1 = '''
            SELECT league, 
                pitcher_name,
                COUNT(*) AS count,
                GROUP_CONCAT(pitch_result) AS pitch_results
            FROM all_plays
            '''
    pitch_query2 = '''
            SELECT league,
                name,
                SUM(batters_faced) AS batters_faced,
                SUM(pitches_thrown) AS pitches_thrown,
                SUM(strike_outs) AS strike_outs,
                SUM(base_on_balls) AS walks,
                SUM(strike_outs) / CAST(SUM(base_on_balls) AS REAL) AS k_bb
            FROM pitchers
            '''
    builder1 = QueryBuilder(pitch_query1)
    builder2 = QueryBuilder(pitch_query2)
    builder1.add_name(name, 'pitcher_name')
    builder2.add_name(name, 'name')
    for b in (builder1, builder2):
        b.add_all_but_name(league, dates, year, game_type)
        b.finish_query()
    combined_overall = get_overall_stats(builder1.get_query(), builder2.get_query(), builder1.get_args())
    if len(combined_overall) == 1:
        return combined_overall[0]
    return combined_overall


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


if __name__ == '__main__':
    add_pitcher_league_averages('A+')
    # print(get_pitcher_data('Ronel Blanco', 'MLB', dates=('2024-03-20', '2024-05-04')))
