import sqlite3
import math
from utils import DataRow, DataRowContainer, connect

pitch_names = [
    'Sinker', 'Slider', 'Changeup', 'Curveball', 'Cutter', '4-Seam Fastball',
    'Splitter', 'Sweeper', 'Knuckle Curve', 'Knuckle Ball', 'Slurve', 'Slow Curve',
    'Fastball', 'Eephus', 'Forkball', 'Screwball', '2-Seam Fastball'
]


def basic_pitch_calcs(data: DataRowContainer[DataRow]):
    name = data[0]['pitcher_name']
    strikeouts = 0
    walks = 0
    d_dict = data.sort_by('game_pk')
    innings = 0
    batters_faced = 0
    er = 0
    for game_pk, container in d_dict.items():
        query = 'SELECT * FROM all_plays WHERE game_pk = ? AND team_fielding = ? GROUP BY inning, ab_number'
        conn, cursor = connect()
        cursor.row_factory = sqlite3.Row
        cursor.execute(query, (game_pk, container.get('team_fielding')[0]))
        plays = cursor.fetchall()
        conn.close()
        for index, play in enumerate(plays):
            if play['events'] == 'Home Run':
                er += 1
            if 'scores' in play['des']:
                for event in play['des'].split('.'):
                    event: str
                    if 'scores' in event:
                        if index == 0:
                            if 'Error' not in plays[0]['events'] and plays[0]['events'] != 'Passed Ball':
                                er += 1
                            else:
                                print('didnt count')
                        else:
                            event = event.replace('scores', '')
                            batter_name = event.strip()
                            for i in range(index-1, -1, -1):
                                if batter_name == plays[i]['batter_name']:
                                    if plays[i]['pitcher_name'] == name:
                                        if 'Error' not in plays[i]['events'] and plays[i]['events'] != 'Passed Ball':
                                            er += 1
                                        else:
                                            print(play['des'])
                                            print(plays[i]['des'])
                                            print('didnt count either')
                                    break

        innings += len(set(container.get('inning')))

        batters_faced += len(set(container.get('ab_number')))
        for d in container.sort_by('ab_number').values():
            events = d.get('events')
            if 'Strikeout' in events:
                strikeouts += 1
            elif 'Walk' in events or 'Hit By Pitch' in events or 'Balk' in events or 'Intent Walk' in events:
                walks += 1
    k = strikeouts / batters_faced * 100 if batters_faced > 0 else 0
    walk_p = walks / batters_faced * 100 if batters_faced > 0 else 0
    era = (9 * er) / innings
    return {'IP': innings, 'ERA': '{:.2f}'.format(era), 'K %': '{:.2f}'.format(k), 'BB %': '{:.2f}'.format(walk_p)}


def get_matching_search(name: str, league: str, team: str, dates: tuple, player_type: str) -> list[sqlite3.Row]:
    conditions = [dates[0], dates[1]]
    if player_type == 'pitcher':
        query = f'SELECT DISTINCT pitcher_name, league, team_fielding FROM all_plays WHERE date BETWEEN ? AND ?'
        if name:
            query += ' AND LOWER(pitcher_name) LIKE ?'
            conditions.append('%' + name.lower() + '%')
        if league:
            query += ' AND LOWER(league) = ?'
            conditions.append(league.lower())
        if team:
            query += ' AND LOWER(team_fielding) = ?'
            conditions.append(team.lower())
        query += ' GROUP BY pitcher_name, league'
    else:
        query = f'SELECT DISTINCT batter_name, league, team_batting FROM all_plays WHERE date BETWEEN ? AND ?'
        if name:
            query += ' AND LOWER(batter_name) LIKE ?'
            conditions.append('%' + name.lower() + '%')
        if league:
            query += ' AND LOWER(league) = ?'
            conditions.append(league.lower())
        if team:
            query += ' AND LOWER(team_batting) = ?'
            conditions.append(team.lower())
        query += ' GROUP BY batter_name, league'
    conn, cursor = connect()
    cursor.execute(query, conditions)
    rows = cursor.fetchall()
    conn.close()
    result = []
    current_name = None
    for index, tup in enumerate(rows):
        name, league, team = tup

        if name != current_name:
            if index != len(rows) - 1:
                if rows[index+1][0] == name:
                    result.append((name, '', ''))
            current_name = name

        result.append(tup)

    return result


def get_pitcher_data(name: str, league: str, dates: tuple) -> DataRowContainer[DataRow]:
    args = [dates[0], dates[1], name]
    pitch_query = f'''
            SELECT *
            FROM all_plays
            WHERE date BETWEEN ? AND ? AND pitcher_name = ?
        '''
    if league:
        pitch_query += ' AND league = ?'
        args.append(league)
    conn, cursor = connect()
    cursor.row_factory = sqlite3.Row
    cursor.execute(pitch_query, args)
    rows = cursor.fetchall()
    conn.close()
    pitcher_data = DataRowContainer([DataRow(r) for r in rows])
    return pitcher_data


def pitch_calcs(name: str, rows: DataRowContainer[DataRow]) -> tuple:
    descriptions = rows.get('description')
    strikes = 0
    balls = 0
    swinging_strikes = 0
    for d in descriptions:
        if 'strike' in d.lower() or 'foul tip' in d.lower() or 'swinging pitchout' in d.lower():
            strikes += 1
        if 'swinging' in d.lower() or 'foul tip' in d.lower():
            swinging_strikes += 1
        if 'ball' in d.lower() or 'hit by' in d.lower() or 'pitchout' in d.lower():
            balls += 1
    return (
        name, len(rows),
        '{:.2f}'.format(sum(rows.get('start_speed')) / len(rows.get('start_speed')) if rows.get('start_speed') else 0),
        '{:.2f}'.format(max(rows.get('start_speed')) if rows.get('start_speed') else 0),
        '{:.2f}'.format(sum(rows.get('spin_rate')) / len(rows.get('spin_rate')) if rows.get('spin_rate') else 0),
        '{:.2f}'.format(sum(rows.get('breakZ')) / len(rows.get('breakZ')) if rows.get('breakZ') else 0),
        '{:.2f}'.format(sum(rows.get('breakX')) / len(rows.get('breakX')) if rows.get('breakX') else 0),
        '{:.2f}'.format(strikes / len(rows) * 100),
        '{:.2f}'.format(swinging_strikes / len(rows) * 100),
        '{:.2f}'.format((len(rows) - balls) / len(rows) * 100)
    )


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


def calc_release_pos(data: DataRow) -> tuple[float, float]:
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
