import requests
import typing
import traceback
import datetime as dt
import time
import threading
import logging
import re
from tqdm import tqdm
from static_data import db_keys, sport_ids, reversed_sport_ids, all_leagues, game_types
log = logging.getLogger()

'''
MAJOR CHANGE
going to request from mlb instead of baseball savant.
https://statsapi.mlb.com/api/v1.1/game/751428/feed/live
'''

BASE_URL = 'https://statsapi.mlb.com/api/v1.1/game/'


unwanted_keys = ['call', 'matchup', 'pitchIndex', 'actionIndex', 'runnerIndex', 'runners', 'playEvents',
                 'playEndTime', 'half_inning', 'reviewDetails', 'hasReview', 'fullName', 'id']


def date_iterator(start_date, end_date=None):
    if end_date is None:
        end_date = str(dt.date.today())
    current_date = dt.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = dt.datetime.strptime(end_date, "%Y-%m-%d")
    td = end_date - current_date
    progress_bar = tqdm(total=td.days, unit='iteration')
    while current_date <= end_date:
        progress_bar.update(1)
        yield current_date.strftime("%Y-%m-%d")
        current_date += dt.timedelta(days=1)
    progress_bar.close()


def https_request(url, end=False):
    try:
        response = requests.get(url)
        response.raise_for_status()  # raise an exception for non-2xx status codes
        data = response.json()
        time.sleep(.2)
        return data
    except requests.exceptions.RequestException as e:
        log.error(f'An error occurred: {e}')
        if e.response:
            if e.response.status_code == 502:
                if end is False:
                    log.info('Trying url again')
                    d = https_request(url, True)
                    return d
                else:
                    log.info('Unsuccessful second attempt')
        return None


# our standard for date format 2023-06-18
def get_pks_over_time(start_date, end_date=None, league=None, debugger=None) -> dict[str, list[int]]:

    game_pks = {
        'MLB': [],
        'AAA': [],
        'AA': [],
        'A+': [],
        'A': [],
        'ROOKIE BALL': [],
        'WINTER LEAGUE': []
    }
    threads = []
    for date in date_iterator(start_date, end_date):
        thread = threading.Thread(target=get_game_pks, args=(game_pks, date, league))
        threads.append(thread)
        thread.start()
        time.sleep(.2)
    for thread in threads:
        thread.join()
    debugger.increment('Fetch', 'games_to_fetch', sum(len(lst) for lst in game_pks.values()))
    return game_pks


def get_game_pks(game_pks: dict, date=None, league: list = None):
    if league is None:
        league = all_leagues
    for x in league:
        sport_id = sport_ids[x]
        url = f'https://statsapi.mlb.com/api/v1/schedule/?sportId={sport_id}&date={str(date)}'
        data = https_request(url)
        game_pks[x].extend(extract_game_pk(data))


def extract_game_pk(data: dict) -> list[int]:
    dates = data['dates']
    game_pks = []
    for x in dates:
        for game in x['games']:
            game_pk = game.get('gamePk')
            game_pks.append(game_pk)
    return game_pks


def get_game_data(game_pk) -> dict:
    url = BASE_URL + str(game_pk) + '/feed/live'  # change games
    data = https_request(url)
    return data


def camel_to_snake(camel_case):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', camel_case).lower()


def flatten_dict(d):
    flat_dict = {}
    for key, value in d.items():
        if key in unwanted_keys:
            continue
        if isinstance(value, dict):
            if key == 'type':
                flat_dict['pitch_code'] = value['code']
                flat_dict['pitch_name'] = value['description']
                value.pop('code')
                value.pop('description')
            elif key == 'details':
                flat_dict['pitch_result'] = value['description']
                flat_dict['pitch_result_code'] = value['code']
                value.pop('description')
                value.pop('code')
            elif key == 'final_count':
                flat_dict['final_balls'] = value['balls']
                flat_dict['final_strikes'] = value['strikes']
                flat_dict['final_outs'] = value['outs']
            nested_dict = flatten_dict(value)
            flat_dict.update(nested_dict)
        else:
            key = camel_to_snake(key)
            if key == 'index':
                key = 'play_events_index'
            if key not in db_keys:
                print(f'Key not in our defaults: {key}: {value}')
            flat_dict[key] = value
    return flat_dict


def get_team_data(team):
    return {
        'name': team['name'],
        'id': team['id'],
        'team_abbr': team['abbreviation']
    }


def extract_game_data(data):
    game_data = data['gameData']
    my_game_data = {
        'game_pk': game_data['game']['pk'],
        'game_type': game_data['game']['type'],
        'season': game_data['game']['season'],
        'date': game_data['datetime']['originalDate'],
        'day_night': game_data['datetime']['dayNight'],
        'time': game_data['datetime']['time'] + game_data['datetime']['ampm'],
        'venue_name': game_data['venue']['name'],
        'venue_id': game_data['venue']['id'],
        'city': game_data['venue']['location']['city'],
        'time_zone': game_data['venue']['timeZone']['tz'],
        'turf_type': game_data['venue']['fieldInfo']['turfType'],
        'weather_condition': game_data['weather'].get('condition'),
        'temp': game_data['weather'].get('temp'),
        'wind': game_data['weather'].get('wind'),
        'official_scorer': game_data.get('officialScorer', {}).get('fullName')
    }
    teams = game_data['teams']
    sport_id = game_data['teams']['home']['sport']['id']
    if sport_id in reversed_sport_ids.keys():
        league = reversed_sport_ids[sport_id]
    else:
        return []
    home_team = get_team_data(teams['home'])
    away_team = get_team_data(teams['away'])

    live_data = data['liveData']
    my_all_plays = []

    for ab_play in live_data['plays']['allPlays']:
        match = ab_play['matchup']
        people_data = {
            'batter_name': match['batter']['fullName'],
            'batter_id': match['batter']['id'],
            'bat_side': match['batSide']['code'],
            'pitcher_name': match['pitcher']['fullName'],
            'pitcher_id': match['pitcher']['id'],
            'pitch_hand': match['pitchHand']['code'],
        }

        runner_index = ab_play['runnerIndex']
        runners = {
            'runner_on_1st': 1 in runner_index,
            'runner_on_2nd': 2 in runner_index,
            'runner_on_3rd': 3 in runner_index
        }

        my_data = []
        pitch_index = ab_play['pitchIndex']

        for i in pitch_index:
            play = {k: None for k in db_keys.keys()}
            if ab_play['playEvents'][i]['isPitch']:
                play.update(flatten_dict(ab_play['playEvents'][i]))
                my_data.append(play)

        ab_play['final_count'] = ab_play['count']
        ab_play.pop('count')
        at_bat_data = flatten_dict(ab_play)

        for x in my_data:
            x.update(at_bat_data)
            x.update(people_data)
            x.update(runners)
            x.update(my_game_data)

            if x['is_top_inning']:
                batters = away_team
                fielders = home_team
            else:
                batters = home_team
                fielders = away_team
            x.update({
                'team_batting': batters['name'],
                'team_batting_id': batters['id'],
                'team_batting_abbr': batters['team_abbr'],
                'team_fielding': fielders['name'],
                'team_fielding_id': fielders['id'],
                'team_fielding_abbr': fielders['team_abbr'],
                'league': league,
                'game_type_name': game_types[x['game_type']]
            })
            my_all_plays.append(x)
    return my_all_plays


def get_plays(pk_dict: dict[str, list[int]], debugger) -> typing.Generator[list[dict], None, None]:
    keys = pk_dict.keys()
    for league in keys:
        for pk in pk_dict[league]:
            try:
                d = get_game_data(pk)
                if d is None:
                    debugger.increment('Fetch', 'games_failed')
                    yield 'failed'
                    continue
                plays = extract_game_data(d)
                if plays:
                    debugger.increment('Fetch', 'games_succeeded')
                    yield plays
                else:
                    debugger.increment('Fetch', 'empty_games')
                    yield 'failed'
            except Exception:
                print(pk)
                debugger.increment('Fetch', 'games_failed')
                print(' There was a ERROR, but that is ok :)')
                traceback.print_exc()
                yield 'failed'

