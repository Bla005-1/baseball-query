import connexion
from pitch_data import get_pitcher_data, basic_pitch_calcs
from batter_data import get_batter_data, basic_batt_calcs
app = connexion.App(__name__, specification_dir='./')
app.add_api('api.yml')


def get_pitcher(league, name, start_date, end_date):
    print(league, name, start_date, end_date)
    if league == 'None':
        league = None
    r = get_pitcher_data(name, league, (start_date, end_date))
    return r


def get_batter(league, name, start_date, end_date):
    print(league, name, start_date, end_date)
    if league == 'None':
        league = None
    r = get_batter_data(name, league, (start_date, end_date))
    return r


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
