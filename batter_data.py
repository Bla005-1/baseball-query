import time
import typing
from utils import connect, select_data
from common_data import add_percentile, calculate_contacts


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
        descriptions = batter_row.get('pitch_results', '').split(',')
        zones = batter_row.get('zones', '')
        if zones is None:
            zones = []
        else:
            zones = zones.split(',')
        percents = calculate_contacts(descriptions, zones)
        batter_row['bb'] = len([x for x in hit_speeds if x is not None])
        batter_row.update({k: v * 100 for k, v in percents.items()})
        batter_row.pop('zones')
        batter_row.pop('pitch_results')
        processed_data.append(batter_row)
    return processed_data


def basic_batt_calcs(name: str, league: str, dates: typing.Tuple[str, str]) -> typing.Dict:
    start_time = time.time()
    args = [dates[0], dates[1], name]
    batt_query = '''
        SELECT 
            SUM(runs) AS runs,
            SUM(doubles) AS doubles,
            SUM(triples) AS triples,
            SUM(home_runs) AS home_runs,
            SUM(strike_outs) AS strike_outs,
            SUM(base_on_balls) AS base_on_balls,
            SUM(hits) AS hits,
            SUM(at_bats) AS at_bats,
            SUM(stolen_bases) AS stolen_bases,
            SUM(plate_appearances) AS plate_appearances,
            SUM(total_bases) AS total_bases,
            SUM(rbi) AS rbi,
            SUM(sac_flies) AS sac_flies,
            SUM(hit_by_pitch) AS hit_by_pitch,
            SUM(games_played) AS games
        FROM hitters
        WHERE date BETWEEN ? AND ? AND name = ?
    '''
    if league:
        batt_query += ' AND league = ?'
        args.append(league)
    data = select_data(batt_query, args)[0]
    games = data['games']
    if games is None:
        return None
    sac_f = data['sac_flies']
    pa = data['plate_appearances']
    ab = data['at_bats']
    hr = data['home_runs']
    doubles = data['doubles']
    triples = data['triples']
    walks = data['base_on_balls']
    strikeout = data['strike_outs']
    hit_by_pitch = data['hit_by_pitch']
    hits = data['hits']
    singles = hits - hr - triples - doubles
    obp = (hits + walks + hit_by_pitch) / (ab + hit_by_pitch + walks + sac_f)
    slg = (singles + doubles*2 + triples*3 + hr*4) / ab
    ba = hits / ab
    strikeout_percent = strikeout / pa * 100
    walk_percent = walks / pa * 100
    print('basic_batt_calcs: ', time.time() - start_time)
    return {'PA': pa, 'AB': ab, 'BA': '{:.4f}'.format(ba), 'OBP': '{:.4f}'.format(obp), 'SLG': '{:.4f}'.format(slg),
            'HR': hr, 'R': data['runs'], 'RBI': data['rbi'], 'SB': data['stolen_bases'],
            'K%': '{:.2f}'.format(strikeout_percent), 'BB%': '{:.2f}'.format(walk_percent), 'singles': singles,
            'doubles': doubles, 'triples': triples, 'H': hits, 'BB': walks, 'G': games,
            'SO': strikeout, 'bases': data['total_bases']}


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
    print(basic_batt_calcs('Freddie Freeman', 'MLB', ('2023-03-27', '2024-05-03')))
