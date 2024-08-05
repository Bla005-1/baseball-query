import json
import queue
import sqlite3
import threading
import datetime as dt
import time
import pandas as pd
import gspread
import os
import traceback
import logging
from typing import *
from tqdm import tqdm
from queue import Queue
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from .utils import DebugManager, connect, select_data, DB_DIR
from .https import get_plays, get_pks_over_time
from .batter_data import get_batter_data, add_batter_league_averages
from .pitch_data import get_pitcher_data, add_pitcher_league_averages, pitcher_per_pitch_calcs
from .static_data import db_keys, hitter_db_keys, pitcher_db_keys, fielder_db_keys, sport_ids, all_leagues


# date format is 2023-08-04
logging.basicConfig(
    filename=os.path.join(DB_DIR, 'daily_update.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger()
debug = DebugManager()
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

data_queue = Queue(maxsize=120)
failed_queue = Queue()

google = os.environ.get('GOOGLE', False)


def create_table(table: str, key_relation: Dict) -> None:
    columns = ', '.join([f"{column} {data_type}" for column, data_type in key_relation.items()])
    if table == 'all_plays':
        indexes = ['batter_name', 'date', 'league', 'pitch_name', 'pitcher_name',
                   'team_fielding', 'game_pk', 'inning', 'at_bat_index', 'game_type']
    else:
        columns += ', PRIMARY KEY (name, game_pk)'
        indexes = ['name', 'date', 'game_pk', 'game_type']
    table_query = f'CREATE TABLE IF NOT EXISTS {table} ({columns})'
    conn, cursor = connect()
    cursor.execute(table_query)
    conn.commit()
    index_query = f'CREATE INDEX IF NOT EXISTS idx_{table}_blank ON {table} (blank);'
    for idx in indexes:
        cursor.execute(index_query.replace('blank', idx))
    conn.commit()
    conn.close()


# p.hit_speed IS NOT NULL AND
def get_initial_data(game_type: str) -> Tuple[List, List, List]:
    year = '2024'
    batter_metrics = ['batter_name', 'league', 'pitches', 'bip', 'percentile_90', 'avg_ev', 'max_ev', 'avg_hit_angle',
                      'contact_percent', 'zone_contact', 'chase_percent', 'swing_percent', 'zone_swing_percent',
                      'barrel_per_bbe']
    batter_df = get_batter_data(metrics=batter_metrics, league=all_leagues, game_type=game_type, year=year)
    pitcher_metrics = ['league', 'pitcher_name', 'count', 'batters_faced', 'pitches_thrown', 'strike_outs', 'walks',
                       'k_bb', 'strike_percent', 'csw_percent', 'swstr_percent', 'ball_percent']
    overall_pitcher_df = get_pitcher_data(metrics=pitcher_metrics, league=all_leagues, game_type=game_type, year=year)
    pitch_query = '''
        SELECT league, 
            pitcher_name, 
            pitch_name, 
            COUNT(*) AS count,
            AVG(CAST(start_speed AS REAL)) AS avg_velo,
            MAX(CAST(start_speed AS REAL)) AS max_velo,
            AVG(CAST(spin_rate AS REAL)) AS avg_spin,
            AVG(CAST(pfx_z AS REAL)) AS v_break,
            AVG(CAST(pfx_x AS REAL)) AS h_break,
            GROUP_CONCAT(pitch_result) AS pitch_results
        FROM all_plays 
        WHERE game_type = ? AND date LIKE ?
        GROUP BY league, pitcher_id, pitch_name
        '''
    pitch_data = pd.DataFrame(select_data(pitch_query, [game_type, year+'%']))
    pitch_results_split = pitch_data['pitch_results'].str.split(',')
    percents = pitch_results_split.apply(pitcher_per_pitch_calcs)
    percents_df = pd.DataFrame(percents.tolist())
    pitcher_df = pd.concat([pitch_data.drop(columns=['pitch_results']), percents_df], axis=1)
    return batter_df, pitcher_df, overall_pitcher_df


def retrieve_data(pk_dict: Dict) -> None:
    for data in get_plays(pk_dict, debug):
        data_queue.put(data)


def execute_query(cursor, query: str, values: Tuple) -> bool:
    try:
        cursor.execute(query, values)
        return True
    except sqlite3.IntegrityError:
        debug.increment(f'DB', 'overwritten_values')
        return False


def process_single_batch(table: str, batch_data: List[Dict], columns: List[str], cursor) -> None:
    q_values = ['?' for _ in columns]
    query = f'INSERT INTO {table} ({", ".join(columns)}) VALUES ({", ".join(q_values)})'
    for item in batch_data:
        for key, value in item.items():
            if isinstance(value, str):
                if ',' in value:
                    item[key] = value.replace(',', ';')
                if '.--' in value:
                    item[key] = None
        sorted_item = {column: item.get(column, None) for column in columns}
        execute_query(cursor, query, tuple(sorted_item.values()))
    cursor.close()


def threaded_batch_processing(table: str, batch_data: List[Dict], columns: List[str]) -> None:
    try:
        conn, cursor = connect()
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.execute('PRAGMA synchronous=NORMAL;')
        conn.execute('BEGIN TRANSACTION')
        try:
            process_single_batch(table, batch_data, columns, cursor)
        except Exception:
            log.error('Error inserting', exc_info=True)
        finally:
            conn.commit()
            conn.close()
    except sqlite3.OperationalError:
        failed_queue.put((table, batch_data, columns))
        print('DB was locked')
        time.sleep(5)


def process_batches(total: int) -> None:
    progress_bar = tqdm(total=total, unit='iteration')
    count = 0
    threads = []
    while count < total:
        play_batch = []
        hitter_batch = []
        pitcher_batch = []
        fielder_batch = []
        if data_queue.full():
            batch_size = 115
        else:
            batch_size = 30
        for i in range(batch_size):
            try:
                data = data_queue.get(timeout=1)
            except queue.Empty:
                break
            progress_bar.update(1)
            count += 1
            if data == 'failed':
                data_queue.task_done()
                continue
            plays, players = data
            play_batch.extend(plays)
            if not players:
                continue
            hitter_batch.extend(players['hitters'])
            pitcher_batch.extend(players['pitchers'])
            fielder_batch.extend(players['fielders'])
            data_queue.task_done()
            debug.increment('DB', 'counted_games')
        debug.increment('DB', 'total_plays', len(play_batch))
        tables = [
            ('all_plays', play_batch, db_keys),
            ('hitters', hitter_batch, hitter_db_keys),
            ('pitchers', pitcher_batch, pitcher_db_keys),
            ('fielders', fielder_batch, fielder_db_keys)
        ]
        for table, batch_data, db_k in tables:
            keys = [str(x) for x in db_k.keys()]
            thread = threading.Thread(target=threaded_batch_processing, args=(table, batch_data, keys))
            thread.start()
            threads.append(thread)
        if len(threads) >= 8:
            for thread in threads[:4]:
                thread.join()
                threads.remove(thread)
    progress_bar.close()
    for thread in threads:
        thread.join()


def initialize_threads(pk_dict: Dict[str, List[int]]) -> None:
    total = sum(len(lst) for lst in pk_dict.values())
    retrieve_threads = []
    for d, v in pk_dict.items():
        mid = len(v) // 2
        thread1 = threading.Thread(target=retrieve_data, args=({d: v[:mid]},))
        thread2 = threading.Thread(target=retrieve_data, args=({d: v[mid:]},))
        thread1.start()
        retrieve_threads.append(thread1)
        thread2.start()
        retrieve_threads.append(thread2)
    insert_thread = threading.Thread(target=process_batches, args=(total,))
    insert_thread.start()

    for thread in retrieve_threads:
        thread.join()
    insert_thread.join()
    failed = []
    while True:
        try:
            f = failed_queue.get(timeout=1)
            failed.append(f)
        except queue.Empty:
            break
    if failed:
        with open('failed.json', 'w') as f:
            json.dump(failed, f)
    print(' ')


def write_to_sheet(df: pd.DataFrame, key: str, sheet: str = 'Sheet1') -> None:
    credentials = Credentials.from_service_account_file(os.path.join(DB_DIR, 'baseball-stats-394502-c0dd81e75f98.json'),
                                                        scopes=SCOPES)

    gc = gspread.authorize(credentials)

    gs = gc.open_by_key(key)
    worksheet1 = gs.worksheet(sheet)
    worksheet1.clear()
    set_with_dataframe(worksheet=worksheet1, dataframe=df, include_index=False,
                       include_column_header=True, resize=True)


def add_to_google():
    batt, pitch, pitch_overall = get_initial_data('R')
    if len(batt) != 0 or len(pitch) != 0:
        print('adding regular to drive')
        batt_regular = '1dyrqFVcnK9034WZHwKmPCLgopqhgeucvwjSD38pTE2Q'
        pitch_regular = '1BdpuSnjGqYZp6RypI-z2NSST1REJsusM42dSkOk-bxc'
        pitch_overall_regular = '1G6fGNiRaxfjBrwHXQU9JEU8awjQYPZHoq_xFc0_574M'
        write_to_sheet(batt, batt_regular)
        write_to_sheet(pitch, pitch_regular)
        write_to_sheet(pitch_overall, pitch_overall_regular)
    write_last_update()


def write_last_update() -> None:
    with open(os.path.join(DB_DIR, 'last_update.txt'), 'w') as last_update:
        last_update.write(str(dt.date.today()))


def daily_update(start_date: None | dt.date | str = None, do_google: bool = True) -> None:
    if start_date is None:
        try:
            with open(os.path.join(DB_DIR, 'last_update.txt'), 'r') as last_update:
                date_str = last_update.read()
                start_date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        except FileNotFoundError:
            start_date = dt.date.today() - dt.timedelta(days=1)
        start_date = start_date - dt.timedelta(days=1)
    log.info(f'Using {start_date} as the beginning of new requests')
    print(f'Using {start_date} as the beginning of new requests')
    the_pk_dict = get_pks_over_time(str(start_date), debugger=debug)
    initialize_threads(the_pk_dict)
    print(debug)
    try:
        for key in sport_ids.keys():
            add_batter_league_averages(key)
            add_pitcher_league_averages(key)
        log.info('Added league averages')
        print('Added league averages')
    except Exception:
        log.error('Error with league averages:', exc_info=True)
        traceback.print_exc()
    batt, pitch, pitch_overall = get_initial_data('R')
    print(batt)
    if do_google:
        try:
            add_to_google()
            log.info('Successfully added to google')
            print('Complete')
        except Exception:
            log.error('Error with google:', exc_info=True)
            traceback.print_exc()
            write_last_update()
            exit(1)
    else:
        write_last_update()
