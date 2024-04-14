import sqlite3
import re
from utils import connect, nested_list, dict_factory


def extract_scoring_batters(input_string):
    pattern = r'\.\s*(.*?)\s+scores\b'

    matches = re.findall(pattern, input_string)
    matches = [match.strip() for match in matches]
    return matches


def create_era_table():
    conn, cursor = connect()
    query = '''
        CREATE TABLE IF NOT EXISTS era_pointers (
            pitcher_name TEXT,
            batter_name TEXT,
            play_id_hit TEXT,
            play_id_scored TEXT,
            date TEXT,
            game_pk TEXT,
            league TEXT,
            PRIMARY KEY (pitcher_name, batter_name, play_id_hit, play_id_scored)
        )
    '''
    cursor.execute(query)
    conn.commit()
    conn.close()
    print('Created ERA table')


def find_era_plays(start_date: str, end_date: str):
    era_plays = []
    in_play = ['Single', 'Double', 'Triple']
    conn, cursor = connect()
    query = '''
            SELECT pitcher_name, batter_name, events, des, date, game_pk, league, play_id, inning, ab_number
            FROM all_plays
            WHERE date BETWEEN ? AND ?
            ORDER BY game_pk, ab_number
        '''
    cursor.row_factory = dict_factory
    cursor.execute(query, (start_date, end_date))
    rows = cursor.fetchall()
    conn.close()
    print(len(rows))
    nested_rows = nested_list(rows)
    current_ab = None
    for game in nested_rows:
        game: list[list[list]]
        for inning in game:
            on_base = []
            for play in inning:
                ab_number = play['ab_number']
                pitcher_name = play['pitcher_name']
                batter_name = play['batter_name']
                event = play['events']
                des = play['des']
                if current_ab == ab_number or des is None or play['play_id'] is None:
                    continue
                current_ab = ab_number

                batters = []
                if 'scores' in des:
                    batters = extract_scoring_batters(des)
                for i, item in enumerate(on_base):
                    pitcher, batter, play_id = item
                    if batter_name == batter:  # remove batters hitting twice in an inning
                        on_base.pop(i)
                    if batter in batters:
                        era_plays.append((pitcher, batter, play_id, play['play_id'],
                                          play['date'], play['game_pk'], play['league']))
                if 'error' not in des.lower() and 'error' not in event.lower():
                    if 'reaches' in des or 'to 1st' in des:
                        on_base.append([pitcher_name, batter_name, play['play_id']])
                    elif event in in_play:
                        on_base.append([pitcher_name, batter_name, play['play_id']])
                    elif event == 'Home Run':
                        era_plays.append((pitcher_name, batter_name, play['play_id'], play['play_id'],
                                          play['date'], play['game_pk'], play['league']))

    return era_plays


def insert_era_plays(era_plays):
    create_era_table()
    conn, cursor = connect()
    query = 'INSERT INTO era_pointers ' \
            '(pitcher_name, batter_name, play_id_hit, play_id_scored, date, game_pk, league) ' \
            'VALUES (?, ?, ?, ?, ?, ?, ?)'
    conn.execute('BEGIN TRANSACTION')
    for play in era_plays:
        try:
            cursor.execute(query, play)
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()
    print('Inserted ERA plays into table')


if __name__ == '__main__':
    insert_era_plays(find_era_plays('2024-02-01', '2024-04-14'))
