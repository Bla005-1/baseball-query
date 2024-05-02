import time
import typing
from utils import connect, select_data
from common_data import add_percentile, calculate_contacts
from static_data import at_bat_events


def get_batter_data(name: str, league: str, dates: typing.Tuple[str, str]) -> typing.Dict:
    args = [dates[0], dates[1], name]
    batt_query = '''
        SELECT
            batter_name,
            league,
            pitch_name,
            COUNT(*) AS seen,
            AVG(CAST(launch_speed AS REAL)) AS avg_velo,
            MAX(CAST(launch_speed AS REAL)) AS max_velo,
            AVG(CAST(launch_angle AS REAL)) AS avg_hit_angle,
            GROUP_CONCAT(zone) as zones,
            GROUP_CONCAT(description) as descriptions,
            GROUP_CONCAT(launch_speed) as percentile_90
        FROM all_plays
        WHERE date BETWEEN ? AND ? AND batter_name = ?
        AND pitch_name IS NOT NULL
    '''
    if league:
        batt_query += ' AND league = ?'
        args.append(league)

    total_query = batt_query.replace('pitch_name,', '"Total" AS pitch_name,')
    batt_query += 'GROUP BY pitch_name'
    args.extend(args)
    rows = select_data(batt_query + '\nUNION ALL\n' + total_query, args)
    processed_rows = process_batter_rows(rows)
    return processed_rows


def process_batter_rows(batter_data: typing.List[typing.Dict]):
    processed_data = []
    for batter_row in batter_data:
        hit_speeds = batter_row.get('percentile_90', '')
        if hit_speeds is None:
            hit_speeds = ''
        hit_speeds = hit_speeds.split(',')  # don't let the dict key name confuse you
        batter_row = add_percentile(batter_row)
        descriptions = batter_row.get('descriptions', '').split(',')
        zones = batter_row.get('zones', '').split(',')
        contact_percent, zone_contact, chase_percent = calculate_contacts(descriptions, zones)
        batter_row['bb'] = len([x for x in hit_speeds if x is not None])
        batter_row['contact_percent'] = contact_percent
        batter_row['zone_contact'] = zone_contact
        batter_row['chase'] = chase_percent
        batter_row.pop('zones')
        batter_row.pop('descriptions')
        processed_data.append(batter_row)
    return processed_data


def basic_batt_calcs(name: str, league: str, dates: typing.Tuple[str, str]) -> typing.Dict:
    start_time = time.time()
    args = [dates[0], dates[1], name]
    batt_query = '''
        SELECT game_pk, at_bat_index, inning, p_x, p_z, strike_zone_top, strike_zone_bottom, pitch_result, 
        description, event 
        FROM all_plays
        WHERE date BETWEEN ? AND ? AND batter_name = ?
    '''
    if league:
        batt_query += ' AND league = ?'
        args.append(league)
    batt_query += ' ORDER BY game_pk, inning, at_bat_index'
    rows = select_data(batt_query, args)
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
        if 'play' in row['pitch_result']:
            contact += 1
        if row['p_x'] is not None:
            if row['p_x'] < -17/2 or row['p_x'] > 17/2 or row['p_z'] < row['strike_zone_bottom'] or row['p_z'] > row['strike_zone_top']:
                outside_pitches += 1
                if row['pitch_result'] in ['Swinging Strike', 'Foul', 'Foul Tip']:
                    missed_swings += 1
        if row['at_bat_index'] != current_ab:
            current_ab = row['at_bat_index']
            pa += 1
            event = row['event']
            des = row['description']
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
    query = f'SELECT description FROM all_plays WHERE (description LIKE "%{name} steals%" OR description LIKE "%{name}  steals%") ' \
            'AND date BETWEEN ? AND ? GROUP BY game_pk, at_bat_index'
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
            SUM(CASE WHEN LOWER(ab_result) LIKE '%foul%' OR LOWER(ab_result) LIKE '%play%' THEN 1 ELSE 0 END) AS bb,
            AVG(CAST(launch_speed AS REAL)) AS avg_velo,
            MAX(CAST(launch_speed AS REAL)) AS max_velo,
            AVG(CAST(launch_angle AS REAL)) AS avg_hit_angle,
            CAST(SUM(CASE WHEN ab_result LIKE '%In play%' OR ab_result = 'Foul'
            THEN 1 ELSE 0 END) AS REAL) / SUM(CASE WHEN ab_result LIKE '%In play%' OR ab_result LIKE '%Foul%' 
            OR ab_result LIKE '%Swinging%' THEN 1 ELSE 0 END) * 100.0 AS contact_percent,
            GROUP_CONCAT(IFNULL(launch_speed, 'None'), ',') AS percentile_90
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
            MAX(CAST(launch_speed AS REAL)) - MIN(CAST(launch_speed AS REAL)) AS hit_speed_diff,
            (MAX(CAST(launch_speed AS REAL)) - MIN(CAST(launch_speed AS REAL))) * 2 AS max_hit_speed_diff,
            MAX(CAST(launch_angle AS REAL)) - MIN(CAST(launch_angle AS REAL)) AS launch_angle_diff,
            100 AS a,
            40 AS b
        FROM all_plays
        WHERE date BETWEEN ? AND ? {'AND league IN ({})'.format(', '.join(['?']*len(leagues)))} 
        AND pitch_name IS NOT NULL
        {'GROUP BY pitch_name' if not total else ''}
    '''


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
