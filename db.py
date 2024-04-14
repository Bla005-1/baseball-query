import queue
import sqlite3
import threading
import time

import numpy as np
import datetime as dt
import pandas as pd
import gspread
import os
import sys
import traceback
import logging
from tqdm import tqdm
from https import get_plays, get_pks_over_time, get_game_pks, date_iterator
from queue import Queue
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials


logging.basicConfig(
    filename='daily_update.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger()

SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

data_queue = Queue()

db_keys = {'play_id': 'TEXT PRIMARY KEY', 'inning': 'INTEGER', 'ab_number': 'INTEGER', 'cap_index': 'INTEGER',
           'outs': 'INTEGER', 'batter': 'INTEGER', 'stand': 'TEXT', 'batter_name': 'TEXT', 'pitcher': 'INTEGER',
           'p_throws': 'TEXT', 'pitcher_name': 'TEXT', 'team_batting': 'TEXT', 'team_fielding': 'TEXT',
           'team_batting_id': 'INTEGER', 'team_fielding_id': 'INTEGER', 'result': 'TEXT', 'des': 'TEXT',
           'events': 'TEXT', 'strikes': 'INTEGER', 'balls': 'INTEGER', 'pre_strikes': 'INTEGER',
           'pre_balls': 'INTEGER', 'call': 'TEXT', 'call_name': 'TEXT', 'pitch_type': 'TEXT',
           'pitch_name': 'TEXT',
           'description': 'TEXT', 'result_code': 'TEXT', 'pitch_call': 'TEXT', 'is_strike_swinging': 'INTEGER',
           'balls_and_strikes': 'TEXT', 'start_speed': 'INTEGER', 'end_speed': 'REAL', 'sz_top': 'REAL',
           'sz_bot': 'REAL', 'extension': 'REAL', 'plateTime': 'REAL', 'zone': 'INTEGER', 'spin_rate': 'INTEGER',
           'px': 'REAL', 'pz': 'REAL', 'x0': 'REAL', 'y0': 'REAL', 'z0': 'REAL', 'ax': 'REAL', 'ay': 'REAL',
           'az': 'REAL', 'vx0': 'REAL', 'vy0': 'REAL', 'vz0': 'REAL', 'pfxX': 'REAL', 'pfxZ': 'REAL',
           'pfxZWithGravity': 'REAL', 'pfxZWithGravityNice': 'INTEGER', 'pfxZDirection': 'TEXT',
           'pfxXWithGravity': 'INTEGER', 'pfxXNoAbs': 'TEXT', 'pfxXDirection': 'TEXT', 'breakX': 'INTEGER',
           'breakZ': 'INTEGER', 'inducedBreakZ': 'INTEGER', 'is_bip_out': 'TEXT', 'pitch_number': 'INTEGER',
           'player_total_pitches': 'INTEGER', 'player_total_pitches_pitch_types': 'INTEGER',
           'game_total_pitches': 'INTEGER', 'rowId': 'TEXT', 'game_pk': 'TEXT', 'player_name': 'TEXT',
           'date': 'TEXT',
           'league': 'TEXT', 'homeRunBallparks': 'TEXT', 'averageLaunchSpeedPlayer': 'TEXT',
           'maxLaunchSpeedPlayer': 'TEXT', 'launchSpeedPlayerRank': 'TEXT', 'averageLaunchSpeedLeague': 'TEXT',
           'maxLaunchSpeedLeague': 'TEXT', 'launchSpeedLeagueRank': 'TEXT', 'hit_speed_round': 'TEXT',
           'hit_speed': 'TEXT', 'hit_distance': 'TEXT', 'xba': 'TEXT', 'hit_angle': 'TEXT',
           'is_barrel': 'INTEGER',
           'hc_x': 'REAL', 'hc_x_ft': 'REAL', 'hc_y': 'REAL', 'hc_y_ft': 'REAL', 'runnerOn1B': 'INTEGER',
           'runnerOn2B': 'INTEGER', 'runnerOn3B': 'INTEGER'}


def connect():
    conn = sqlite3.connect('baseball_plays.db')
    cursor = conn.cursor()
    return conn, cursor


def process_batter_rows(rows: list) -> list[dict]:
    batter_data = []
    for row in rows:
        league = row[0]
        name = row[1]
        play_data = row[2].split(',')

        # remove trailing empty string caused by GROUP_CONCAT
        if play_data[-1] == '':
            play_data.pop()

        interval = 4
        play_data = [None if value == 'None' else value for value in play_data]
        hit_speeds = [float(play_data[x]) for x in range(0, len(play_data), interval) if play_data[x] is not None]
        hit_angles = [float(play_data[x]) for x in range(1, len(play_data), interval) if play_data[x] is not None]
        descriptions = [play_data[x] for x in range(2, len(play_data), interval)]
        contact = 0
        total = 0
        bb = 0
        all_strike = 0
        for d in descriptions:
            if d.lower() == 'foul' or 'in play' in d.lower():
                contact += 1
                total += 1
            if 'foul tip' in d.lower() or 'swinging' in d.lower():
                total += 1
            if 'play' in d.lower():
                bb += 1
            if 'strike' in d.lower():
                all_strike += 1
        average_velocity = sum(hit_speeds) / len(hit_speeds) if hit_speeds else 0
        max_ev = max(hit_speeds) if hit_speeds else 0
        avg_launch_angle = sum(hit_angles) / len(hit_angles) if hit_angles else 0
        if total == 0:
            contact_percent = 0
        else:
            contact_percent = (contact / total) * 100
        percentile_90 = np.percentile(hit_speeds, 90) if hit_speeds else 0

        batter_data.append(
            {
                'league': league,
                'name': name,
                'exit_velocity': "{:.2f}".format(average_velocity),
                'max_ev': "{:.2f}".format(max_ev),
                'avg_launch_angle': "{:.2f}".format(avg_launch_angle),
                'bb': bb,
                'contact_percent': "{:.2f}".format(contact_percent),
                'percentile_90': "{:.2f}".format(percentile_90)
            })
    return batter_data


def process_pitch_rows(pitch_rows: list) -> list[dict]:
    pitcher_data = []
    for play in pitch_rows:
        league = play[0]
        name = play[1]
        play_data = play[2].split(',')
        # remove trailing empty string caused by GROUP_CONCAT
        if play_data[-1] == '':
            play_data.pop()

        interval = 10
        play_data = [None if value == 'None' else value for value in play_data]
        v_breaks = [play_data[x] for x in range(0, len(play_data), interval)]
        h_breaks = [play_data[x] for x in range(1, len(play_data), interval)]
        descriptions = [play_data[x] for x in range(2, len(play_data), interval)]
        results = [play_data[x] for x in range(4, len(play_data), interval)]
        ab_number = [float(play_data[x]) for x in range(5, len(play_data), interval) if play_data[x] is not None]
        pks = [play_data[x] for x in range(6, len(play_data), interval)]
        pitch_names = [play_data[x] for x in range(7, len(play_data), interval)]
        spin_rates = [play_data[x] for x in range(8, len(play_data), interval)]
        velocities = [play_data[x] for x in range(9, len(play_data), interval)]
        all_strikes = 0
        all_balls = 0
        swinging_strikes = 0
        for d in descriptions:
            if 'strike' in d.lower() or 'foul tip' in d.lower() or 'swinging pitchout' in d.lower():
                all_strikes += 1
            if 'swinging' in d.lower() or 'foul tip' in d.lower():
                swinging_strikes += 1
            if 'ball' in d.lower() or 'hit by' in d.lower() or 'pitchout' in d.lower():
                all_balls += 1

        per_pitch = {'Total': {
            'count': 0,
            'velo': [],
            'spin_rate': [],
            'v_break': [],
            'h_break': [],
            'strikes': 0,
            'swstr': 0,
            'balls': 0
        }}
        for i in range(len(pitch_names)):
            p_name = pitch_names[i]
            if p_name is None:
                p_name = 'None'
            index = i
            d = descriptions[index]
            v = velocities[index]
            s = spin_rates[index]
            p = per_pitch.get(p_name)
            t = per_pitch.get('Total')
            v_break = v_breaks[index]
            h_break = h_breaks[index]
            t['count'] += 1
            if p is not None:
                p['count'] += 1
                if 'strike' in d.lower() or 'foul tip' in d.lower() or 'swinging pitchout' in d.lower():
                    p['strikes'] += 1
                    t['strikes'] += 1
                if 'swinging' in d.lower() or 'foul tip' in d.lower():
                    p['swstr'] += 1
                    t['swstr'] += 1
                if 'ball' in d.lower() or 'hit by' in d.lower() or 'pitchout' in d.lower():
                    p['balls'] += 1
                    t['balls'] += 1
                if v:
                    p['velo'].append(float(v))
                    t['velo'].append(float(v))
                if s:
                    p['spin_rate'].append(float(s))
                    t['spin_rate'].append(float(s))
                if v_break:
                    p['v_break'].append(float(v_break) * 2)
                    t['v_break'].append(float(v_break) * 2)
                if h_break:
                    p['h_break'].append(float(h_break) * 2)
                    t['h_break'].append(float(h_break) * 2)
            else:
                per_pitch[p_name] = {
                    'count': 1,
                    'velo': [float(v)] if v else [],
                    'spin_rate': [float(s)] if s else [],
                    'v_break': [float(v_break) * 2] if v_break else [],
                    'h_break': [float(h_break) * 2] if v_break else [],
                    'strikes': 0,
                    'swstr': 0,
                    'balls': 0
                }
                if 'strike' in d.lower() or 'foul tip' in d.lower() or 'swinging pitchout' in d.lower():
                    t['strikes'] += 1
                if 'swinging' in d.lower() or 'foul tip' in d.lower():
                    t['swstr'] += 1
                if 'ball' in d.lower() or 'hit by' in d.lower() or 'pitchout' in d.lower():
                    t['balls'] += 1
                if v:
                    t['velo'].append(float(v))
                if s:
                    t['spin_rate'].append(float(s))
                if v_break:
                    t['v_break'].append(float(v_break) * 2)
                if h_break:
                    t['h_break'].append(float(h_break) * 2)
        abats = []
        strikeout = 0
        walk = 0
        for i in range(len(results)):
            ab = ab_number[i]
            r = results[i]
            pk = pks[i]
            y = str(ab) + str(pk)
            if y not in abats:
                abats.append(y)
                if 'strikeout' in r.lower():
                    strikeout += 1
                if 'walk' in r.lower():
                    walk += 1
        csw = all_strikes / len(descriptions) * 100
        swstr = swinging_strikes / len(descriptions) * 100
        strike_percent = (len(descriptions) - all_balls) / len(descriptions) * 100
        ball_percent = all_balls / len(descriptions) * 100
        strikeout_percent = strikeout / len(abats) * 100
        walk_percent = walk / len(abats) * 100
        kbb = strikeout_percent - walk_percent
        for pitch_type, values in per_pitch.items():
            for v in values.keys():
                if isinstance(values[v], list):
                    if len(values[v]) == 0:
                        values[v] = [0]
            pitcher_data.append(
                {
                    'league': league,
                    'name': name,
                    'total_pitches': len(descriptions),
                    'csw': '{:.2f}'.format(csw),
                    'swstr': '{:.2f}'.format(swstr),
                    'strike_percent': '{:.2f}'.format(strike_percent),
                    'ball_percent': '{:.2f}'.format(ball_percent),
                    'strikeout_percent': '{:.2f}'.format(strikeout_percent),
                    'walk_percent': '{:.2f}'.format(walk_percent),
                    'k-bb': '{:.2f}'.format(kbb),
                    'pitch_type': pitch_type,
                    'count': int(values['count']),
                    'Avg. Velo': '{:.2f}'.format(sum(values['velo']) / len(values['velo']) if values['velo'] else 0),
                    'Max Velo': '{:.2f}'.format(max(values['velo'])),
                    'Avg. Spin': '{:.2f}'.format(sum(values['spin_rate']) / len(values['spin_rate'])),
                    'V break': '{:.2f}'.format(sum(values['v_break']) / len(values['v_break'])),
                    'H break': '{:.2f}'.format(sum(values['h_break']) / len(values['h_break'])),
                    'CSW %': '{:.2f}'.format(values['strikes'] / values['count'] * 100),
                    'SwStr': '{:.2f}'.format(values['swstr'] / values['count'] * 100),
                    'Strike %': '{:.2f}'.format((values['count'] - values['balls']) / values['count'] * 100)
                })

    return pitcher_data


# p.hit_speed IS NOT NULL AND
def get_initial_data(dates: tuple, batt_query: str = None, pitch_query: str = None) -> tuple[list, list]:
    conn, cursor = connect()
    if batt_query is None:
        batt_query = '''
            SELECT league, batter_name,
                GROUP_CONCAT(IFNULL(CAST(hit_speed AS REAL), 'None') || ',' || IFNULL(CAST(hit_angle AS REAL), 'None') 
                || ',' || IFNULL(REPLACE(description, ',', '-'), 'None') || ',' ||
                IFNULL(REPLACE(des, ',', '-'), 'None')) AS play_data
            FROM all_plays
            WHERE des NOT LIKE '%bunt%' AND date BETWEEN ? AND ?
            GROUP BY league, batter_name;
        '''
    if pitch_query is None:
        pitch_query = '''
                SELECT league, pitcher_name,
                    GROUP_CONCAT(IFNULL(CAST(pfxZ AS REAL), 'None') || ',' || IFNULL(CAST(pfxX AS REAL), 'None') 
                    || ',' || IFNULL(REPLACE(description, ',', '-'), 'None') || ',' || 
                    IFNULL(REPLACE(des, ',', '-'), 'None') || ',' || IFNULL(result, 'None') || ',' || 
                    IFNULL(ab_number, 'None')
                    || ',' || IFNULL(game_pk, 'None') || ',' || IFNULL(pitch_name, 'None') || ',' || 
                    IFNULL(spin_rate, 'None') || ',' || IFNULL(start_speed, 'None')) AS play_data
                FROM all_plays
                WHERE des NOT LIKE '%bunt%' AND date BETWEEN ? AND ?
                GROUP BY league, pitcher_name;
            '''
    cursor.execute(batt_query, dates)
    batt_rows = cursor.fetchall()
    cursor.execute(pitch_query, dates)
    pitch_rows = cursor.fetchall()
    batter_data = process_batter_rows(batt_rows)
    pitcher_data = process_pitch_rows(pitch_rows)
    conn.close()
    return batter_data, pitcher_data


def retrieve_data(pk_dict, output_queue: queue.Queue):
    for plays in get_plays(pk_dict, output_queue):
        data_queue.put(plays)


counted_games = 0
overwritten_plays = 0
total_plays = 0


def insert_batch_data(batch: list[list[dict]]):
    conn, cursor = connect()
    columns = [str(x) for x in db_keys.keys()]
    q_values = ['?' for x in columns]
    query = f'INSERT INTO all_plays ({", ".join(columns)}) VALUES ({", ".join(q_values)})'
    global counted_games, total_plays, overwritten_plays
    for b in batch:
        counted_games += 1
        conn.execute('BEGIN TRANSACTION')
        for item in b:
            total_plays += 1
            for key, value in item.items():
                if isinstance(value, str) and ',' in value:
                    item[key] = value.replace(',', ';')
            sorted_item = {}
            for column in columns:
                if column == 'date':
                    date = item.get(column)
                    date_str = []
                    for x in date.split('/'):
                        if len(x) < 2:
                            x = '0' + x
                        date_str.append(x)
                    sorted_item[column] = date_str[2] + '-' + date_str[0] + '-' + date_str[1]
                else:
                    sorted_item[column] = item.get(column, None)
            try:
                cursor.execute(query, tuple(sorted_item.values()))

            except sqlite3.IntegrityError:
                overwritten_plays += 1
        conn.commit()
    conn.close()


def insert_queue_data(total):
    progress_bar = tqdm(total=total, unit='iteration')
    count = 0
    while count < total:
        batch = []
        for i in range(30):
            try:
                data = data_queue.get(timeout=10)
            except queue.Empty:
                break
            progress_bar.update(1)
            count += 1
            if data == 'failed':
                data_queue.task_done()
                continue
            batch.append(data)
            data_queue.task_done()

        insert_batch_data(batch)

    progress_bar.close()


def check_db(start_date, end_date):
    pk_dict = get_pks_over_time(start_date, end_date)
    all_pks = []
    for pk_list in pk_dict.values():
        all_pks.extend(pk_list)

    conn, cursor = connect()

    query = 'SELECT DISTINCT game_pk FROM all_plays WHERE date BETWEEN ? AND ?'
    cursor.execute(query, (start_date, end_date))
    rows = cursor.fetchall()
    rows = [int(x[0]) for x in rows]
    missing = []
    found = []
    duplicate = []
    print(all_pks)
    print(rows)
    for pk in all_pks:
        if pk not in rows:
            missing.append(pk)
        else:
            if pk in found:
                duplicate.append(pk)
            found.append(pk)
    print(missing)
    print(duplicate)
    print('Duplicate: ', len(duplicate))
    print('Total got: ', len(all_pks))
    print('Missing: ', len(missing))


def initialize_threads(pk_dict: dict[str: list[int]]):
    total = sum(len(lst) for lst in pk_dict.values())
    retrieve_threads = []
    output_queue = queue.Queue()
    for d, v in pk_dict.items():
        mid = len(v) // 2
        thread1 = threading.Thread(target=retrieve_data, args=({d: v[:mid]}, output_queue))
        thread2 = threading.Thread(target=retrieve_data, args=({d: v[mid:]}, output_queue))
        print(f'created thread for {d}')
        thread1.start()
        retrieve_threads.append(thread1)
        thread2.start()
        retrieve_threads.append(thread2)

    insert_thread = threading.Thread(target=insert_queue_data, args=(total,))
    insert_thread.start()

    for thread in retrieve_threads:
        thread.join()

    print(' ')
    while not output_queue.empty():
        print(output_queue.get())

    data_queue.join()
    insert_thread.join()
    print('Counted games: ', counted_games)
    print('Total plays: ', total_plays)
    print('Overwritten plays: ', overwritten_plays)
    print('All threads finished')


def write_to_sheet(df, key: str, sheet: str = 'Sheet1'):
    credentials = Credentials.from_service_account_file('baseball-stats-394502-c0dd81e75f98.json', scopes=SCOPES)

    gc = gspread.authorize(credentials)

    gs = gc.open_by_key(key)
    worksheet1 = gs.worksheet(sheet)
    worksheet1.clear()
    set_with_dataframe(worksheet=worksheet1, dataframe=df, include_index=False,
                       include_column_header=True, resize=True)


def add_to_google():
    if dt.date.today() < dt.date(2024, 4, 1):
        pre_batt, pre_pitch = get_initial_data(('2024-01-01', '2024-03-27'))
        df1 = pd.DataFrame.from_records(pre_batt)
        df2 = pd.DataFrame.from_records(pre_pitch)
        batt_spr_train = '1sa_hUSrkka1K8WbNJ23HVtLoYDCzNT3HRQXfHsbjbR4'
        pitch_spr_train = '11LdxxpWHjnWZinEn-4jSSnx0hYrmNK5jeQem1dq_Qlo'
        print('adding spring training to drive')
        write_to_sheet(df1, batt_spr_train)
        write_to_sheet(df2, pitch_spr_train)

    batt, pitch = get_initial_data(('2024-03-28', '2024-12-31'))
    if len(batt) != 0 or len(pitch) != 0:
        df3 = pd.DataFrame.from_records(batt)
        df4 = pd.DataFrame.from_records(pitch)
        print('adding regular to drive')
        batt_regular = '1dyrqFVcnK9034WZHwKmPCLgopqhgeucvwjSD38pTE2Q'
        pitch_regular = '1BdpuSnjGqYZp6RypI-z2NSST1REJsusM42dSkOk-bxc'
        write_to_sheet(df3, batt_regular)
        write_to_sheet(df4, pitch_regular)

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

    log.info(f'Using {start_date} as the beginning of new requests')
    print(f'Using {start_date} as the beginning of new requests')
    the_pk_dict = get_pks_over_time(str(start_date))
    initialize_threads(the_pk_dict)
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
    # check_db('2023-01-01', '2024-02-24')
    try:
        os.chdir(os.path.dirname(__file__))
        if len(sys.argv) == 2:
            try:
                s_d = dt.datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
                daily_update(start_date=s_d, google=False)
            except ValueError:
                print('Format using YYYY-MM-DD')
                exit(1)
        else:
            daily_update(google=False)
    except Exception:
        log.error('An error occurred:', exc_info=True)
        exit(1)
