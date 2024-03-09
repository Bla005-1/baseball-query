import connexion
from db import connect
from pitch_data import get_pitcher_data, basic_pitch_calcs

app = connexion.App(__name__, specification_dir='./')
app.add_api('api.yml')


def get_pitcher(league, name, start_date, end_date):
    print(league, name, start_date, end_date)
    if league == 'None':
        league = None
    r = get_pitcher_data(name, league, (start_date, end_date))
    basic_calcs = basic_pitch_calcs(r)
    return basic_calcs


def get_batter(league, name, start_date, end_date):
    return {'league': 'MLB', 'pitcher_name': 'b dude'}


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
