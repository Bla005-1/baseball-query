import sqlite3
import time
import numpy as np
from utils import connect, dict_factory

at_bat_events = [
    'Double', 'Strikeout', 'Flyout', 'Single', 'Forceout', 'Pop Out', 'Groundout',
    'GIDP', 'Field Error', 'Lineout', 'Fielders Choice', 'Double Play',
    'Catcher Interference', 'Hit By Pitch', 'Home Run', 'Stolen Base 3B', 'Triple',
    'Fielders Choice Out', 'Batter Out', 'Field Out'
]


def get_batter_data(name: str, league: str, dates: tuple) -> dict:
    args = [dates[0], dates[1], name]
    batt_query = '''
        SELECT
            pitch_name,
            COUNT(*) AS seen,
            SUM(CASE WHEN LOWER(description) LIKE '%foul%' OR LOWER(description) LIKE '%play%' THEN 1 ELSE 0 END) AS bb,
            AVG(CAST(hit_speed AS REAL)) AS avg_velo,
            MAX(CAST(hit_speed AS REAL)) AS max_velo,
            AVG(CAST(hit_angle AS REAL)) AS avg_hit_angle,
            CAST(SUM(CASE WHEN description LIKE '%In play%' OR description = 'Foul'
            THEN 1 ELSE 0 END) AS REAL) / SUM(CASE WHEN description LIKE '%In play%' OR description LIKE '%Foul%' 
            OR description LIKE '%Swinging%' THEN 1 ELSE 0 END) * 100.0 AS contact_percent,
            GROUP_CONCAT(IFNULL(hit_speed, 'None'), ',') AS percentile_90
        FROM all_plays
        WHERE date BETWEEN ? AND ?
        AND pitch_name IS NOT NULL
    '''
    if league:
        batt_query += ' AND league = ?'
        args.append(league)

    total_query = batt_query.replace('pitch_name,', '"Total" AS pitch_name,')
    batt_query += 'GROUP BY pitch_name'

    conn, cursor = connect()
    cursor.row_factory = dict_factory
    cursor.execute(batt_query + '\nUNION ALL\n' + total_query, args)
    rows = cursor.fetchall()
    conn.close()
    return rows


def batt_calcs(name: str, rows: DataRowContainer[DataRow]) -> tuple:
    descriptions = rows.get('description')
    contact = 0
    total = 0
    bb = 0
    all_strike = 0
    for d in descriptions:
        if d.lower() == 'foul' or 'in play' in d.lower():
            contact += 1
            total += 1
        if 'foul tip' in d.lower() or 'swinging' in d.lower():
            total += 1
        if 'play' in d.lower():
            bb += 1
        if 'strike' in d.lower():
            all_strike += 1
    hit_speeds = [float(x) for x in rows.get('hit_speed')]
    hit_angles = [float(x) for x in rows.get('hit_angle')]
    average_velocity = sum(hit_speeds) / len(hit_speeds) if hit_speeds else 0
    max_ev = max(hit_speeds) if hit_speeds else 0
    avg_launch_angle = sum(hit_angles) / len(hit_angles) if hit_angles else 0
    if total == 0:
        contact_percent = 0
    else:
        contact_percent = (contact / total) * 100
    percentile_90 = np.percentile(hit_speeds, 90) if hit_speeds else 0

    return (
            name, len(rows), bb,
            "{:.2f}".format(average_velocity),
            "{:.2f}".format(max_ev),
            "{:.2f}".format(avg_launch_angle),
            "{:.2f}".format(contact_percent),
            "{:.2f}".format(percentile_90)
        )


def basic_batt_calcs(data: DataRowContainer[DataRow]):
    start_time = time.time()
    name = data.get('batter_name')[0]
    sac_b = 0
    sac_f = 0
    pa = 0
    ab = 0
    rbi = 0
    hr = 0
    singles = 0
    doubles = 0
    triples = 0
    walks = 0
    strikeout = 0
    missed_swings = 0
    outside_pitches = 0
    sz_top = sum(data.get('sz_top'))/len(data)
    sz_bottom = sum(data.get('sz_bot'))/len(data)
    for row in data:
        if row['px'] is not None:
            if row['px'] < -17/2 or row['px'] > 17/2:
                outside_pitches += 1
            elif row['pz'] < sz_bottom or row['pz'] > sz_top:
                outside_pitches += 1
            else:
                continue
            if row['description'] in ['Swinging Strike', 'Foul', 'Foul Tip']:
                missed_swings += 1

    hits = len([x for x in data.get('description') if 'play' in x])
    hit_by_pitch = len([x for x in data.get('description') if 'Hit By Pitch' == x])
    for game_data in data.sort_by('game_pk').values():
        for at_bat in game_data.sort_by('ab_number').values():
            pa += 1
            event = at_bat.get('events')[0]
            if event in at_bat_events:
                ab += 1
            if event == 'Single':
                singles += 1
            elif event == 'Double':
                doubles += 1
            elif event == 'Triple':
                triples += 1
            elif event == 'Home Run':
                hr += 1
            elif event == 'Sac Fly':
                sac_f += 1
            elif event == 'Sac Bunt':
                sac_b += 1
            if event != 'Field Error' and event != 'GIDP':
                des = at_bat.get('des')[0]
                rbi += des.count('scores')
            if event == 'Walk' or event == 'Hit By Pitch' or event == 'Balk' or event == 'Intent Walk':
                walks += 1
            if 'Strikeout' in event:
                strikeout += 1
    rbi += hr
    obp = (hits + walks + hit_by_pitch) / (pa - sac_b) * 100
    slg = (singles + doubles*2 + triples*3 + hr*4) / pa
    ba = (singles + doubles + triples + hr + sac_f + sac_b) / ab
    strikeout_percent = strikeout / pa * 100
    walk_percent = walks / pa * 100
    chase_percent = missed_swings / outside_pitches * 100
    conn, cursor = connect()
    query = f'SELECT COUNT(*) FROM all_plays WHERE des LIKE "%{name} steals%" OR des LIKE "%{name}  steals%"'
    cursor.execute(query)
    steals = cursor.fetchone()
    query = f'SELECT COUNT(*) FROM all_plays WHERE des LIKE "%{name} scores%" OR des LIKE "%{name}  scores%"'
    cursor.execute(query)
    runs = cursor.fetchone()
    conn.close()
    print('basic_batt_calcs: ', time.time() - start_time)
    return {'PA': pa, 'BA': '{:.4f}'.format(ba), 'OBP': '{:.2f}'.format(obp), 'SLG': '{:.2f}'.format(slg),
            'HR': hr, 'R': runs, 'RBI': rbi, 'SB': steals, 'K%': '{:.2f}'.format(strikeout_percent),
            'BB%': '{:.2f}'.format(walk_percent), 'Chase%': '{:.2f}'.format(chase_percent)}


def build_calcs_query(leagues: list[str], total=False):
    return f'''
        SELECT
            {"'Total' AS pitch_name" if total else 'pitch_name'},
            COUNT(*) AS seen,
            SUM(CASE WHEN LOWER(description) LIKE '%foul%' OR LOWER(description) LIKE '%play%' THEN 1 ELSE 0 END) AS bb,
            AVG(CAST(hit_speed AS REAL)) AS avg_velo,
            MAX(CAST(hit_speed AS REAL)) AS max_velo,
            AVG(CAST(hit_angle AS REAL)) AS avg_hit_angle,
            CAST(SUM(CASE WHEN description LIKE '%In play%' OR description = 'Foul'
            THEN 1 ELSE 0 END) AS REAL) / SUM(CASE WHEN description LIKE '%In play%' OR description LIKE '%Foul%' 
            OR description LIKE '%Swinging%' THEN 1 ELSE 0 END) * 100.0 AS contact_percent,
            GROUP_CONCAT(IFNULL(hit_speed, 'None'), ',') AS percentile_90
        FROM all_plays
        WHERE date BETWEEN ? AND ? {'AND league IN ({})'.format(', '.join(['?']*len(leagues)))} 
        AND pitch_name IS NOT NULL
        {'GROUP BY pitch_name' if not total else ''}
    '''


def build_range_query(leagues: list[str], total=False):
    return f'''
        SELECT
            {"'Total' AS pitch_name" if total else 'pitch_name'},
            COUNT(*) AS seen,
            0,
            MAX(CAST(hit_speed AS REAL)) - MIN(CAST(hit_speed AS REAL)) AS hit_speed_diff,
            (MAX(CAST(hit_speed AS REAL)) - MIN(CAST(hit_speed AS REAL))) * 2 AS max_hit_speed_diff,
            MAX(CAST(hit_angle AS REAL)) - MIN(CAST(hit_angle AS REAL)) AS hit_angle_diff,
            100 AS a,
            40 AS b
        FROM all_plays
        WHERE date BETWEEN ? AND ? {'AND league IN ({})'.format(', '.join(['?']*len(leagues)))} 
        AND pitch_name IS NOT NULL
        {'GROUP BY pitch_name' if not total else ''}
    '''


def add_percentile(row):
    row = list(row)
    if row[-1] is not None:
        hit_speeds = row[-1].split(',')
        hit_speeds = [float(x) for x in hit_speeds if x != 'None']
        row[-1] = np.percentile(hit_speeds, 90) if hit_speeds else 0
    return row


def batt_league_average(leagues: list[str], dates: tuple) -> tuple[dict[str: tuple], dict[str: tuple]]:
    args = [dates[0], dates[1]]
    args.extend(leagues)
    conn, cursor = connect()

    cursor.execute(build_calcs_query(leagues), args)
    batt_calcs_rows = cursor.fetchall()
    batt_calcs_rows = [add_percentile(r) for r in batt_calcs_rows]

    cursor.execute(build_range_query(leagues), args)
    batt_range_rows = cursor.fetchall()

    cursor.execute(build_calcs_query(leagues, True), args)
    total_avg = cursor.fetchone()
    total_avg = add_percentile(total_avg)
    averages = {'Total': tuple(total_avg)}

    cursor.execute(build_range_query(leagues, True), args)
    total_range = cursor.fetchone()
    ranges = {'Total': total_range}

    conn.close()

    averages.update({row[0]: tuple(row) for row in batt_calcs_rows if row[0] is not None})
    ranges.update({row[0]: tuple(row) for row in batt_range_rows if row[0] is not None})
    return averages, ranges
