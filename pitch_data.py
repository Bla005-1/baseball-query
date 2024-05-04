import math
import typing
from utils import connect, select_data
from common_data import calculate_percents


def process_pitch_rows(pitch_data: typing.List[typing.Dict], overall_data: dict = None):
    if overall_data is None:
        overall_data = {}
    processed_data = []
    for pitch_row in pitch_data:
        my_data = {}
        player = overall_data.get(pitch_row['league'] + pitch_row['pitcher_name'])
        descriptions = pitch_row.get('pitch_results', '').split(',')
        percents = calculate_percents(descriptions)
        pitch_row['strike_percent'] = percents['o_strike_percent']
        pitch_row['csw_percent'] = percents['o_csw']
        pitch_row['swstr_percent'] = percents['o_swstr']
        pitch_row.pop('pitch_results')
        my_data.update(player)
        my_data.update(pitch_row)
        processed_data.append(my_data)
    return processed_data


def basic_pitch_calcs(name: str, league: str = None, dates: tuple[str, str] = None, game_type: str = None):
    args = [name]
    query = '''
        SELECT SUM(innings_pitched) AS IP,
            9 * SUM(earned_runs) / SUM(innings_pitched) AS ERA,
            SUM(strike_outs) / SUM(CAST(batters_faced AS REAL)) AS strikeout_ratio,
            SUM(base_on_balls) / SUM(CAST(batters_faced AS REAL)) AS walk_ratio
        FROM pitchers WHERE name = ?
    '''
    if league:
        query += ' AND league = ?'
        args.append(league)
    if dates:
        query += ' AND date BETWEEN ? AND ?'
        args.extend([dates[0], dates[1]])
    if game_type:
        query += ' AND game_type = ?'
        args.append(game_type)
    calcs = select_data(query, args)[0]
    return {k: round(v, 3) for k, v in calcs.items()}


# could make get_data a common function between pitch and batt since only query is different
def get_pitcher_data(name: str, league: str, dates: tuple[str, str]) -> dict:
    args = [dates[0], dates[1], name]
    pitch_query = '''
        SELECT
            pitch_name,
            COUNT(*) AS total_pitches,
            AVG(start_speed) AS avg_start_speed,
            MAX(start_speed) AS max_start_speed,
            AVG(spin_rate) AS avg_spin_rate,
            AVG(breakZ) AS avg_breakZ,
            AVG(breakX) AS avg_breakX,
            SUM(CASE WHEN ab_result LIKE '%Strike%' OR ab_result LIKE '%Foul Tip%' OR 
             ab_result LIKE '%Swinging Pitchout%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS strike_percentage,
            SUM(CASE WHEN LOWER(ab_result) LIKE '%swinging%' OR ab_result
             LIKE '%Foul Tip%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS swinging_strike_percentage,
            SUM(CASE WHEN LOWER(ab_result) LIKE '%ball%' OR LOWER(ab_result) LIKE '%hit by%' OR LOWER(ab_result) 
             LIKE '%pitchout%' THEN 0 ELSE 1 END) * 100.0 / COUNT(*) AS ball_percentage
        FROM all_plays
        WHERE date BETWEEN ? AND ? AND pitcher_name = ?
        AND pitch_name IS NOT NULL
        '''
    if league:
        pitch_query += ' AND league = ?'
        args.append(league)

    total_query = pitch_query.replace('pitch_name,', '"Total" AS pitch_name,')
    pitch_query += 'GROUP BY pitch_name'
    args.extend(args)
    rows = select_data(pitch_query + '\nUNION ALL\n' + total_query, args)
    return rows


def build_calcs_query(leagues: list[str], total=False):
    return f'''
        SELECT
            {"'Total' AS pitch_name" if total else 'pitch_name'},
            COUNT(*) AS total_pitches,
            AVG(start_speed) AS avg_start_speed,
            MAX(start_speed) AS max_start_speed,
            AVG(spin_rate) AS avg_spin_rate,
            AVG(breakZ) AS avg_breakZ,
            AVG(breakX) AS avg_breakX,
            SUM(CASE WHEN LOWER(ab_result) LIKE '%strike%' OR LOWER(ab_result) LIKE '%foul tip%' OR 
             LOWER(ab_result) LIKE '%swinging pitchout%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS strike_percentage,
            SUM(CASE WHEN LOWER(ab_result) LIKE '%swinging%' OR LOWER(ab_result) 
             LIKE '%foul tip%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS swinging_strike_percentage,
            SUM(CASE WHEN LOWER(ab_result) LIKE '%ball%' OR LOWER(ab_result) LIKE '%hit by%' OR LOWER(ab_result) 
             LIKE '%pitchout%' THEN 0 ELSE 1 END) * 100.0 / COUNT(*) AS ball_percentage
        FROM all_plays
        WHERE date BETWEEN ? AND ? {'AND league IN ({})'.format(', '.join(['?']*len(leagues)))} 
        AND pitch_name IS NOT NULL
        {'GROUP BY pitch_name' if not total else ''}
    '''


def build_range_query(leagues: list[str], total=False):
    return f'''
        SELECT
            {"'Total' AS pitch_name" if total else 'pitch_name'},
            COUNT(*) AS total_pitches,
            MAX(start_speed) - MIN(start_speed) AS start_speed_range,
            (MAX(start_speed) - MIN(start_speed)) * 2 AS max_speed_difference,
            MAX(spin_rate) - MIN(spin_rate) AS spin_rate_range,
            MAX(breakZ) - MIN(breakZ) AS breakZ_range,
            MAX(breakX) - MIN(breakX) AS breakX_range,
            100 AS a,
            100 AS b,
            100 AS c
        FROM all_plays
        WHERE date BETWEEN ? AND ? {'AND league IN ({})'.format(', '.join(['?']*len(leagues)))} 
        AND pitch_name IS NOT NULL
        {'GROUP BY pitch_name' if not total else ''}
    '''


def pitch_league_average(leagues: list[str], dates: tuple) -> tuple[dict[str: tuple], dict[str: tuple]]:
    args = [dates[0], dates[1]]
    args.extend(leagues)
    conn, cursor = connect()

    cursor.execute(build_calcs_query(leagues), args)
    pitch_calcs_rows = cursor.fetchall()

    cursor.execute(build_range_query(leagues), args)
    pitch_range_rows = cursor.fetchall()

    cursor.execute(build_calcs_query(leagues, True), args)
    total_avg = cursor.fetchone()
    averages = {'Total': total_avg}

    cursor.execute(build_range_query(leagues, True), args)
    total_range = cursor.fetchone()
    ranges = {'Total': total_range}

    conn.close()

    averages.update({row[0]: tuple(row) for row in pitch_calcs_rows if row[0] is not None})
    ranges.update({row[0]: tuple(row) for row in pitch_range_rows if row[0] is not None})

    return averages, ranges


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
    print(basic_pitch_calcs('Ronel Blanco', 'MLB', ('2024-03-20', '2024-05-04')))
