import sqlite3


def nested_list(rows: list[dict]):
    nested_lists = []
    current_game = None
    current_inning = None
    inning_list = []
    game_list = []
    for row in rows:
        game_pk, inning = row['game_pk'], row['inning']
        if game_pk != current_game:
            if game_list:
                nested_lists.append(game_list)
                game_list = []
            current_game = game_pk
        if inning != current_inning:
            if inning_list:
                game_list.append(inning_list)
            inning_list = [row]
            current_inning = inning
        else:
            inning_list.append(row)

    if inning_list:
        game_list.append(inning_list)
        nested_lists.append(game_list)
    return nested_lists


def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}


def connect():
    conn = sqlite3.connect('C:/Users/bryce/py_projects/baseball_stats/baseball_plays.db')
    cursor = conn.cursor()
    return conn, cursor


def get_matching_search(name: str, league: str, team: str, dates: tuple, player_type: str) -> list[sqlite3.Row]:
    conditions = [dates[0], dates[1]]
    if player_type == 'pitcher':
        query = f'SELECT DISTINCT pitcher_name, league, team_fielding FROM all_plays WHERE date BETWEEN ? AND ?'
        if name:
            query += ' AND LOWER(pitcher_name) LIKE ?'
            conditions.append('%' + name.lower() + '%')
        if league:
            query += ' AND LOWER(league) = ?'
            conditions.append(league.lower())
        if team:
            query += ' AND LOWER(team_fielding) = ?'
            conditions.append(team.lower())
        query += ' GROUP BY pitcher_name, league'
    else:
        query = f'SELECT DISTINCT batter_name, league, team_batting FROM all_plays WHERE date BETWEEN ? AND ?'
        if name:
            query += ' AND LOWER(batter_name) LIKE ?'
            conditions.append('%' + name.lower() + '%')
        if league:
            query += ' AND LOWER(league) = ?'
            conditions.append(league.lower())
        if team:
            query += ' AND LOWER(team_batting) = ?'
            conditions.append(team.lower())
        query += ' GROUP BY batter_name, league'
    conn, cursor = connect()
    cursor.execute(query, conditions)
    rows = cursor.fetchall()
    conn.close()
    result = []
    current_name = None
    for index, tup in enumerate(rows):
        name, league, team = tup

        if name != current_name:
            if index != len(rows) - 1:
                if rows[index+1][0] == name:
                    result.append((name, '', ''))
            current_name = name

        result.append(tup)

    return result
