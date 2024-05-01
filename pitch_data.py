import math
from utils import connect, select_data

pitch_names = [
    'Sinker', 'Slider', 'Changeup', 'Curveball', 'Cutter', '4-Seam Fastball',
    'Splitter', 'Sweeper', 'Knuckle Curve', 'Knuckle Ball', 'Slurve', 'Slow Curve',
    'Fastball', 'Eephus', 'Forkball', 'Screwball', '2-Seam Fastball'
]


def basic_pitch_calcs(name: str, league: str, dates: tuple[str, str]):
    args = [name, dates[0], dates[1]]
    query1 = 'SELECT * FROM era_pointers WHERE pitcher_name = ? AND date BETWEEN ? AND ?'
    query2 = '''
            SELECT pitcher_name, events, outs, inning, game_pk
            FROM all_plays
            WHERE pitcher_name = ? AND date BETWEEN ? AND ?
            '''
    if league:
        query1 += ' AND league = ?'
        query2 += ' AND league = ?'
        args.append(league)
    query2 += ' GROUP BY game_pk, ab_number ORDER BY game_pk, ab_number'
    er_plays = select_data(query1, args, None)
    rows = select_data(query2, args)
    current_inning = None
    current_game = None
    current_out = 0
    ip = 0
    walks = 0
    strikeouts = 0
    batters_faced = 0
    for row in rows:
        batters_faced += 1
        game_pk = row['game_pk']
        outs = row['outs']
        inning = row['inning']
        if game_pk != current_game:
            current_game = game_pk
            current_out = outs
        if inning != current_inning:
            current_out = outs
            current_inning = inning
        if outs != current_out:
            ip += (1/3)
        event = row['events']
        if event == 'Walk':
            walks += 1
        elif 'strikeout' in event.lower():
            strikeouts += 1
    era = 9 * len(er_plays) / ip
    k = strikeouts / batters_faced * 100
    bb = walks / batters_faced * 100
    return {'IP': ip, 'ERA': '{:.2f}'.format(era), 'K%': '{:.2f}'.format(k), 'BB%': '{:.2f}'.format(bb)}


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
            SUM(CASE WHEN description LIKE '%Strike%' OR description LIKE '%Foul Tip%' OR 
             description LIKE '%Swinging Pitchout%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS strike_percentage,
            SUM(CASE WHEN LOWER(description) LIKE '%swinging%' OR description
             LIKE '%Foul Tip%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS swinging_strike_percentage,
            SUM(CASE WHEN LOWER(description) LIKE '%ball%' OR LOWER(description) LIKE '%hit by%' OR LOWER(description) 
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
            SUM(CASE WHEN LOWER(description) LIKE '%strike%' OR LOWER(description) LIKE '%foul tip%' OR 
             LOWER(description) LIKE '%swinging pitchout%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS strike_percentage,
            SUM(CASE WHEN LOWER(description) LIKE '%swinging%' OR LOWER(description) 
             LIKE '%foul tip%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS swinging_strike_percentage,
            SUM(CASE WHEN LOWER(description) LIKE '%ball%' OR LOWER(description) LIKE '%hit by%' OR LOWER(description) 
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
    print(basic_pitch_calcs('Ronel Blanco', 'MLB', ('2024-03-20', '2024-04-14')))
