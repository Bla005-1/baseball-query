import requests
import typing
import traceback
import datetime as dt
import time
import threading
import logging
from tqdm import tqdm

log = logging.getLogger()

BASE_URL = "https://baseballsavant.mlb.com/gf?game_pk="

sport_ids = {
    'MLB': 1,
    'AAA': 11,
    'AA': 12,
    'A+': 13,
    'A': 14,
    'ROOKIE BALL': 16,
    'WINTER LEAGUE': 17
}
all_leagues = ['MLB', 'AAA', 'AA', 'A+', 'A', 'ROOKIE BALL', 'WINTER LEAGUE']


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
        # time.sleep(.2)
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
    url = BASE_URL + str(game_pk)  # change games
    data = https_request(url)
    return data


def format_game_data(data: dict, league: str) -> list[dict]:
    plays: list[dict] = data.get('team_home') + data.get('team_away')
    date: str = data.get('gameDate')
    for play in plays:
        play['date'] = date
        play['league'] = league
        play['homeRunBallparks'] = play.get('contextMetrics', {}).get('homeRunBallparks')
        play['averageLaunchSpeedPlayer'] = play.get('contextMetrics', {}).get('averageLaunchSpeedPlayer')
        play['maxLaunchSpeedPlayer'] = play.get('contextMetrics', {}).get('maxLaunchSpeedPlayer')
        play['launchSpeedPlayerRank'] = play.get('contextMetrics', {}).get('launchSpeedPlayerRank')
        play['averageLaunchSpeedLeague'] = play.get('contextMetrics', {}).get('averageLaunchSpeedLeague')
        play['maxLaunchSpeedLeague'] = play.get('contextMetrics', {}).get('maxLaunchSpeedLeague')
        play['launchSpeedLeagueRank'] = play.get('contextMetrics', {}).get('launchSpeedLeagueRank')
        play.pop('contextMetrics')
    return plays


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
                if d.get('team_home') is None or d.get('team_away') is None:
                    debugger.increment('Fetch', 'games_failed')
                    yield 'failed'
                    continue
                plays = format_game_data(d, league)
                if plays:
                    debugger.increment('Fetch', 'games_succeeded')
                    yield plays
                else:
                    print('uh oh, look at https.get_plays')

            except Exception as e:
                debugger.increment('Fetch', 'games_failed')
                print(' There was a ERROR, but that is ok :)')
                traceback.print_tb(e.__traceback__)
                yield 'failed'

