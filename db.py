import queue
import sqlite3
import threading
import datetime as dt
import pandas as pd
import gspread
import os
import sys
import traceback
import logging
from tqdm import tqdm
from https import get_plays, get_pks_over_time
from queue import Queue
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from utils import DebugManager, connect, select_data
from batter_data import process_batter_rows
from pitch_data import process_pitch_rows, get_overall_stats
from static_data import db_keys, hitter_db_keys, pitcher_db_keys, fielder_db_keys
from common_data import calculate_percents

logging.basicConfig(
    filename='daily_update.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger()
debug = DebugManager()
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

data_queue = Queue(maxsize=100)


def create_table(table: str, key_relation: dict):
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
def get_initial_data(game_type: str) -> tuple[list, list, list]:
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
        WHERE game_type = ?
        GROUP BY league, batter_name
        '''
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
        WHERE game_type = ?
        GROUP BY league, pitcher_name, pitch_name
        '''
    pitch_query2 = '''
        SELECT league, 
            pitcher_name,
            COUNT(*) AS count,
            GROUP_CONCAT(pitch_result) AS pitch_results
        FROM all_plays WHERE game_type = ?
        GROUP BY league, pitcher_name
        '''
    pitch_query3 = '''
        SELECT league,
            name,
            SUM(batters_faced) AS batters_faced,
            SUM(pitches_thrown) AS pitches_thrown,
            SUM(strike_outs) AS strike_outs,
            SUM(base_on_balls) AS walks,
            SUM(strike_outs) / CAST(SUM(base_on_balls) AS REAL) AS k_bb
        FROM pitchers WHERE game_type = ?
        GROUP BY league, name
        '''
    batter_data = select_data(batt_query, [game_type])
    pitch_data = select_data(pitch_query, [game_type])
    combined_overall = get_overall_stats(pitch_query2, pitch_query3, [game_type])
    batter_rows = process_batter_rows(batter_data)
    pitcher_rows = process_pitch_rows(pitch_data)
    return batter_rows, pitcher_rows, combined_overall


def retrieve_data(pk_dict: dict):
    for data in get_plays(pk_dict, debug):
        data_queue.put(data)


def execute_query(cursor, query, values):
    try:
        cursor.execute(query, values)
        return True
    except sqlite3.IntegrityError:
        debug.increment(f'DB', 'overwritten_values')
        return False


def process_single_batch(table, batch_data, columns, cursor):
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


def threaded_batch_processing(table, batch_data, columns):
    conn, cursor = connect()
    conn.execute('BEGIN TRANSACTION')
    try:
        process_single_batch(table, batch_data, columns, cursor)
    finally:
        conn.commit()
        conn.close()


def process_batches(total: int):
    progress_bar = tqdm(total=total, unit='iteration')
    count = 0
    threads = []
    while count < total:
        play_batch = []
        hitter_batch = []
        pitcher_batch = []
        fielder_batch = []
        for i in range(30):
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
            debug.increment('DB', 'total_plays', len(play_batch))
            play_batch.extend(plays)
            if not players:
                continue
            hitter_batch.extend(players['hitters'])
            pitcher_batch.extend(players['pitchers'])
            fielder_batch.extend(players['fielders'])
            data_queue.task_done()
            debug.increment('DB', 'counted_games')

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
        if len(threads) >= 12:
            for thread in threads[:3]:
                thread.join()
                threads.remove(thread)
    progress_bar.close()
    for thread in threads:
        thread.join()


def initialize_threads(pk_dict: dict[str: list[int]]):
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
    print(' ')


def write_to_sheet(df: pd.DataFrame, key: str, sheet: str = 'Sheet1'):
    credentials = Credentials.from_service_account_file('baseball-stats-394502-c0dd81e75f98.json', scopes=SCOPES)

    gc = gspread.authorize(credentials)

    gs = gc.open_by_key(key)
    worksheet1 = gs.worksheet(sheet)
    worksheet1.clear()
    set_with_dataframe(worksheet=worksheet1, dataframe=df, include_index=False,
                       include_column_header=True, resize=True)


def add_to_google():
    batt, pitch, pitch_overall = get_initial_data('R')
    if len(batt) != 0 or len(pitch) != 0:
        df1 = pd.DataFrame.from_records(batt)
        df2 = pd.DataFrame.from_records(pitch)
        df3 = pd.DataFrame.from_records(pitch_overall)
        print('adding regular to drive')
        batt_regular = '1dyrqFVcnK9034WZHwKmPCLgopqhgeucvwjSD38pTE2Q'
        pitch_regular = '1BdpuSnjGqYZp6RypI-z2NSST1REJsusM42dSkOk-bxc'
        pitch_overall_regular = '1G6fGNiRaxfjBrwHXQU9JEU8awjQYPZHoq_xFc0_574M'
        write_to_sheet(df1, batt_regular)
        write_to_sheet(df2, pitch_regular)
        write_to_sheet(df3, pitch_overall_regular)
    write_last_update()


def write_last_update():
    with open('last_update.txt', 'w') as last_update:
        last_update.write(str(dt.date.today()))


def daily_update(start_date=None, google=True):
    if start_date is None:
        try:
            with open('last_update.txt', 'r') as last_update:
                date_str = last_update.read()
                start_date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        except FileNotFoundError:
            start_date = dt.date.today() - dt.timedelta(days=1)
    # start_date = start_date - dt.timedelta(days=1)
    log.info(f'Using {start_date} as the beginning of new requests')
    print(f'Using {start_date} as the beginning of new requests')
    the_pk_dict = get_pks_over_time(str(start_date), debugger=debug)
    initialize_threads(the_pk_dict)
    print(debug)
    if google:
        try:
            add_to_google()
            log.info('Successfully completed todays run')
            print('Complete')
        except Exception:
            log.error('An error occurred:', exc_info=True)
            traceback.print_exc()
            write_last_update()
            exit(1)
    else:
        write_last_update()


# date format is 2023-08-04
if __name__ == '__main__':
    # create_all_plays_table()
    create_table('all_plays', db_keys)
    create_table('hitters', hitter_db_keys)
    create_table('pitchers', pitcher_db_keys)
    create_table('fielders', fielder_db_keys)
    try:
        os.chdir(os.path.dirname(__file__))
        google = True
        if len(sys.argv) == 2:
            try:
                s_d = dt.datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
                daily_update(start_date=s_d, google=google)
            except ValueError:
                print('Format using YYYY-MM-DD')
                exit(1)
        else:
            daily_update(google=google)
    except Exception:
        log.error('An error occurred:', exc_info=True)
        exit(1)
