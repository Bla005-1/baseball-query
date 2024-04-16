import time
import numpy as np
from utils import connect, dict_factory

at_bat_events = [
    'Double', 'Strikeout', 'Flyout', 'Single', 'Forceout', 'Pop Out', 'Groundout',
    'GIDP', 'Field Error', 'Lineout', 'Fielders Choice', 'Double Play', 'Home Run', 'Stolen Base 3B', 'Triple',
    'Fielders Choice Out', 'Batter Out', 'Field Out', 'Strikeout Double Play', 'Bunt Pop Out',
    'Bunt Lineout', 'Bunt Groundout', 'Field Out', 'Batter Out', 'Triple Play', 'Runner Double Play'
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
        WHERE date BETWEEN ? AND ? AND batter_name = ?
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
    rows = [add_percentile(x) for x in rows]
    conn.close()
    return rows


def basic_batt_calcs(name: str, league: str, dates: tuple) -> dict:
    start_time = time.time()
    args = [dates[0], dates[1], name]
    batt_query = '''
        SELECT game_pk, ab_number, inning, px, pz, sz_top, sz_bot, description, des, events  
        FROM all_plays
        WHERE date BETWEEN ? AND ? AND batter_name = ?
    '''
    if league:
        batt_query += ' AND league = ?'
        args.append(league)
    batt_query += ' ORDER BY game_pk, inning, ab_number'
    conn, cursor = connect()
    cursor.row_factory = dict_factory
    cursor.execute(batt_query, args)
    rows = cursor.fetchall()
    print(len(rows))
    conn.close()

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
    contact = 0
    hit_by_pitch = 0
    current_ab = 0
    hit_keywords = ['Single', 'Double', 'Triple', 'Home Run']
    hits = 0
    games = 0
    current_game = None
    for row in rows:
        if current_game != row['game_pk']:
            current_game = row['game_pk']
            games += 1
        if 'play' in row['description']:
            contact += 1
        if row['px'] is not None:
            if row['px'] < -17/2 or row['px'] > 17/2 or row['pz'] < row['sz_bot'] or row['pz'] > row['sz_top']:
                outside_pitches += 1
                if row['description'] in ['Swinging Strike', 'Foul', 'Foul Tip']:
                    missed_swings += 1
        if row['ab_number'] != current_ab:
            current_ab = row['ab_number']
            pa += 1
            event = row['events']
            des = row['des']
            if event in hit_keywords and 'error' not in des:
                hits += 1
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
                rbi += des.count('scores')
            if event == 'Walk' or event == 'Intent Walk':
                walks += 1
            elif event == 'Hit By Pitch':
                hit_by_pitch += 1
            if 'Strikeout' in event:
                strikeout += 1
    rbi += hr
    obp = (hits + walks + hit_by_pitch) / (ab + hit_by_pitch + walks + sac_f)
    slg = (singles + doubles*2 + triples*3 + hr*4) / ab
    ba = hits / ab
    strikeout_percent = strikeout / pa * 100
    walk_percent = walks / pa * 100
    chase_percent = missed_swings / outside_pitches * 100
    conn, cursor = connect()
    query = f'SELECT des FROM all_plays WHERE (des LIKE "%{name} steals%" OR des LIKE "%{name}  steals%") ' \
            'AND date BETWEEN ? AND ? GROUP BY game_pk, ab_number'
    cursor.execute(query, dates)
    steals = cursor.fetchall()
    query = query.replace('steals', 'scores')
    cursor.execute(query, dates)
    runs = cursor.fetchall()
    runs = len(runs)
    steals = len(steals)
    conn.close()
    print('basic_batt_calcs: ', time.time() - start_time)
    return {'PA': pa, 'AB': ab, 'BA': '{:.4f}'.format(ba), 'OBP': '{:.4f}'.format(obp), 'SLG': '{:.4f}'.format(slg),
            'HR': hr, 'R': runs, 'RBI': rbi, 'SB': steals, 'K%': '{:.2f}'.format(strikeout_percent),
            'BB%': '{:.2f}'.format(walk_percent), 'Chase%': '{:.2f}'.format(chase_percent), 'singles': singles,
            'doubles': doubles, 'triples': triples, 'H': hits, 'BB': walks, 'G': games,
            'SO': strikeout}


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


if __name__ == '__main__':
    print(basic_batt_calcs('Freddie Freeman', 'MLB', ('2023-03-27', '2024-01-01')))
