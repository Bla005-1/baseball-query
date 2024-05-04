from static_data import reversed_sport_ids
from utils import camel_to_snake
from static_data import db_keys, game_types


unwanted_keys = ['call', 'matchup', 'pitchIndex', 'actionIndex', 'runnerIndex', 'runners', 'playEvents',
                 'playEndTime', 'half_inning', 'reviewDetails', 'hasReview', 'fullName', 'id']


def get_team_data(team):
    return {
        'name': team['name'],
        'id': team['id'],
        'team_abbr': team['abbreviation']
    }


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


def extract_all_plays(data):
    game_data = data['gameData']
    game_stats = extract_game_stats(game_data)
    if game_stats is None:
        return []
    my_game_data, home_team, away_team = game_stats
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
            })
            my_all_plays.append(x)
    return my_all_plays


# table for hitting, pitching, and fielding
def extract_game_stats(game_data):
    my_game_data = {
        'game_pk': game_data['game']['pk'],
        'game_type': game_data['game']['type'],
        'game_type_name': game_types[game_data['game']['type']],
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
        return None
    home_team = get_team_data(teams['home'])
    away_team = get_team_data(teams['away'])
    my_game_data['league'] = league
    return my_game_data, home_team, away_team


def extract_player_stats(data):
    game_data = data['gameData']
    if game_data['status']['codedGameState'] != 'F':
        print(game_data['game'])
        return []
    game_stats = extract_game_stats(game_data)
    if game_stats is None:
        return []
    my_game_data = game_stats[0]
    teams = [game_stats[1], game_stats[2]]
    my_players = {
        'hitters': [],
        'pitchers': [],
        'fielders': []
    }
    for i, team in enumerate(['home', 'away']):
        players = data['liveData']['boxscore']['teams'][team]['players']
        for p in players.values():
            if p.get('position') is None:
                continue
            my_game_data.update({
                'name': p['person']['fullName'],
                'player_id': p['person']['id'],
                'jersey_number': p.get('jerseyNumber'),
                'position': p['position']['name'],
                'position_code': p['position']['code'],
                'team_name': teams[i]['name'],
                'team_id': teams[i]['id'],
                'team_abbr': teams[i]['team_abbr']
            })
            stats = p['stats']
            if b := stats['batting']:
                my_players['hitters'].append(touch_up_dict(b, my_game_data))
            if p := stats['pitching']:
                my_players['pitchers'].append(touch_up_dict(p, my_game_data))
            if f := stats['fielding']:
                my_players['fielders'].append(touch_up_dict(f, my_game_data))
    return my_players


def touch_up_dict(d: dict, game_data: dict) -> dict:
    d = {camel_to_snake(k): v for k, v in d.items()}
    d.update(game_data)
    return d
