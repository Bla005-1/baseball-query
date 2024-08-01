import requests
import typing
import traceback
import datetime as dt
import time
import threading
import logging
from tqdm import tqdm
from .static_data import sport_ids, all_leagues
from .data_extracting import extract_all_plays, extract_player_stats
log = logging.getLogger()

'''
MAJOR CHANGE
going to request from mlb instead of baseball savant.
https://statsapi.mlb.com/api/v1.1/game/751428/feed/live
'''

BASE_URL = 'https://statsapi.mlb.com/api/v1.1/game/'


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
                plays = extract_all_plays(d)
                if plays:
                    debugger.increment('Fetch', 'games_succeeded')
                    players = extract_player_stats(d)
                    yield plays, players
                else:
                    debugger.increment('Fetch', 'empty_games')
                    yield 'failed'
            except Exception:
                print(pk)
                debugger.increment('Fetch', 'games_failed')
                print(' There was a ERROR, but that is ok :)')
                traceback.print_exc()
                yield 'failed'
