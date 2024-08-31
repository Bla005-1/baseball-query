import json
import queue
import sqlite3
import threading
import datetime as dt
import time
import os
import traceback
import logging
from typing import *
from tqdm import tqdm
from queue import Queue
from .utils import DebugManager, connect, DB_DIR
from .https import get_plays, get_pks_over_time
from .batter_data import add_batter_league_averages
from .pitch_data import add_pitcher_league_averages
from .static_data import db_keys, hitter_db_keys, pitcher_db_keys, fielder_db_keys, sport_ids


log = logging.getLogger()
debug = DebugManager()

data_queue = Queue(maxsize=120)
failed_queue = Queue()


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


def write_last_update() -> None:
    with open(os.path.join(DB_DIR, 'last_update.txt'), 'w') as last_update:
        last_update.write(str(dt.date.today()))


def daily_update(start_date: None | dt.date | str = None) -> None:
    if start_date is None:
        try:
            with open(os.path.join(DB_DIR, 'last_update.txt'), 'r') as last_update:
                date_str = last_update.read()
                start_date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        except FileNotFoundError:
            start_date = dt.date.today() - dt.timedelta(days=1)
        start_date = start_date - dt.timedelta(days=1)
    log.info(f'Using {start_date} as the beginning of new requests')
    the_pk_dict = get_pks_over_time(str(start_date), debugger=debug)
    initialize_threads(the_pk_dict)
    log.info(debug)
    try:
        for key in sport_ids.keys():
            add_batter_league_averages(key)
            add_pitcher_league_averages(key)
        log.info('Added league averages')
    except Exception:
        log.error('Error with league averages:', exc_info=True)
        traceback.print_exc()
    write_last_update()
