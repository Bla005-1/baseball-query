from typing import *
from utils import select_data, QueryBuilder
from common_data import add_percentile, calculate_contacts


def get_batter_data(name: Union[str, List[str]], league: str = None, dates: Tuple[str, str] = None,
                    game_type: str = None) -> Union[List[Dict], Dict]:
    batt_query = '''
        SELECT 
            batter_name,
            league,
            COUNT(*) AS pitches,
            GROUP_CONCAT(IFNULL(zone, 0)) as zones,
            GROUP_CONCAT(pitch_result) as pitch_results,
            SUM(CASE WHEN pitch_result LIKE "%play%" THEN 1 ELSE 0 END) AS bip,
            GROUP_CONCAT(launch_speed) as percentile_90,
            AVG(CAST(launch_speed AS REAL)) AS avg_ev,
            MAX(CAST(launch_speed AS REAL)) AS max_ev,
            AVG(CAST(launch_angle AS REAL)) AS avg_hit_angle
        FROM all_plays
    '''
    builder = QueryBuilder(batt_query)
    builder.add_name(name, 'batter_name')
    builder.add_league(league)
    builder.add_dates(dates)
    builder.add_game_type(game_type)
    builder.finish_query()
    rows = select_data(builder.get_query(), builder.get_args())
    processed_rows = process_batter_rows(rows)
    if len(processed_rows) == 1:
        return processed_rows[0]
    return processed_rows


def process_batter_rows(batter_data: List[Dict]) -> List[Dict]:
    processed_data = []
    for batter_row in batter_data:
        batter_row = add_percentile(batter_row)
        descriptions = batter_row.get('pitch_results', '').split(',')
        zones = batter_row.get('zones', '')
        if zones is None:
            zones = []
        else:
            zones = zones.split(',')
        percents = calculate_contacts(descriptions, zones)
        batter_row.update({k: v * 100 for k, v in percents.items()})
        batter_row.pop('zones')
        batter_row.pop('pitch_results')
        processed_data.append(batter_row)
    return processed_data


def basic_batt_calcs(name: Union[str, List[str]], league: str, dates: Tuple[str, str] = None,
                     game_type: str = None) -> List[Dict]:
    batt_query = '''
            SELECT 
                name,
                league,
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
        '''
    builder = QueryBuilder(batt_query)
    builder.add_name(name)
    builder.add_league(league)
    builder.add_dates(dates)
    builder.add_game_type(game_type)
    builder.finish_query()
    all_data = select_data(builder.get_query(), builder.get_args())
    finished_results = [perform_calcs(x) for x in all_data if x['games'] is not None]
    return finished_results


def perform_calcs(data):
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
    if ab != 0:
        obp = (hits + walks + hit_by_pitch) / (ab + hit_by_pitch + walks + sac_f)
        slg = (singles + doubles * 2 + triples * 3 + hr * 4) / ab
        ba = hits / ab
    else:
        obp = slg = ba = 0
    if pa != 0:
        strikeout_percent = strikeout / pa * 100
        walk_percent = walks / pa * 100
    else:
        strikeout_percent = walk_percent = 0
    return {'name': data['name'], 'PA': pa, 'AB': ab, 'BA': round(ba, 4), 'OBP': round(obp, 4),
            'SLG': round(slg, 4), 'HR': hr, 'R': data['runs'], 'RBI': data['rbi'], 'SB': data['stolen_bases'],
            'K%': round(strikeout_percent, 2), 'BB%': round(walk_percent, 2), 'singles': singles,
            'doubles': doubles, 'triples': triples, 'H': hits, 'BB': walks, 'G': games,
            'SO': strikeout, 'bases': data['total_bases']}


if __name__ == '__main__':
    r = get_batter_data('Freddie Freeman', 'MLB', game_type='R')
    print(r)
