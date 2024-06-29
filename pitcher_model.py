import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from utils import select_data
from pitch_data import get_pitcher_data, basic_pitch_calcs

draft_kings_points = {
    'IP': 2.25,
    'W': 4.0,
    'CG': 2.5,
    'SHO': 2.5,
    'H': -0.6,
    'ER': -2.0,
    'HBP': -0.6,
    'BB': -0.6,
    'K': 2.0
}


def calculate_fan_points(league):
    query = '''
        SELECT name,
            SUM(innings_pitched) AS IP,
            SUM(wins) AS W,
            SUM(complete_games) AS CG,
            SUM(shutouts) AS SHO,
            SUM(hits) AS H,
            SUM(earned_runs) AS ER,
            SUM(hit_by_pitch) AS HBP,
            SUM(base_on_balls) AS BB,
            SUM(strike_outs) AS K
        FROM pitchers
        WHERE game_type = "R" AND league = ?
        GROUP BY name
    '''
    data = select_data(query, (league,))
    for i, row in enumerate(data):
        fan_points = 0
        for k, v in row.items():
            if k == 'name':
                continue
            fan_points += draft_kings_points[k] * v
        data[i] = {'name': row['name'], 'fan_points': fan_points}
    return pd.DataFrame(data)


def pitches_thrown_regression(row: pd.Series) -> pd.Series:
    # Split cumulative_pitches into a list of integers
    pitches = list(map(int, row['cumulative_pitches'].split(',')))

    # Create a DataFrame from these pitch counts
    data = pd.DataFrame({'pitches_thrown': pitches})

    # Calculate cumulative pitches thrown
    data['cum_pitches'] = data['pitches_thrown'].cumsum()

    # Create a 'game' column
    data.insert(0, 'game', range(1, len(data) + 1))

    # Prepare the data for regression
    x = data['game'].values.reshape(-1, 1)
    y = data['cum_pitches'].values

    # Fit the regression model
    model = LinearRegression()
    model.fit(x, y)

    # Get the slope and intercept
    slope = model.coef_[0]
    intercept = model.intercept_

    # Predict the number of pitches for the next game
    prediction = ((len(pitches) + 1) * slope) + intercept
    return prediction


# Load player metrics data (training data)
def load_training_data(league):
    names = select_data('SELECT DISTINCT name FROM pitchers WHERE game_type = "R" AND league = "MLB"')
    names = [r['name'] for r in names]
    training_data = pd.DataFrame(get_pitcher_data(names, league=league))
    extra_training_data = pd.DataFrame(basic_pitch_calcs(names, league=league))
    extra_training_data['predicted_pitches'] = extra_training_data.apply(pitches_thrown_regression, axis=1)
    print(extra_training_data)
    merged_data = pd.merge(training_data, extra_training_data, left_on='name', right_on='name', how='inner')
    return merged_data.dropna()


# Merge the datasets on player's name
def merge_datasets(metrics_data, fantasy_points_data):
    merged_data = pd.merge(metrics_data, fantasy_points_data[['name', 'fan_points']], left_on='name', right_on='name')
    return merged_data


# Train a regression model to learn the relationship between metrics and fantasy points
def train_regression_model(data, metrics):
    x = data[metrics].astype(float)
    y = data['fan_points'].astype(float)

    # Standardize the features
    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(x)

    # Train a RandomForestRegressor
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(x_scaled, y)

    return model, scaler


# Extract feature importances as weights
def extract_weights(model, metrics):
    importances = model.feature_importances_
    weights = {metric: importance for metric, importance in zip(metrics, importances)}

    # Normalize the weights to sum to 1
    total_importance = sum(weights.values())
    normalized_weights = {metric: weight / total_importance for metric, weight in weights.items()}

    return normalized_weights


# Main function to orchestrate the process
def main():
    print('Starting the optimization process')
    league = 'MLB'
    # Load data
    training_data = load_training_data(league)
    fantasy_points_data = calculate_fan_points(league)
    fantasy_points_data = fantasy_points_data.sort_values('fan_points', ascending=False)

    metrics = [
        'ERA', 'fly_outs', 'ground_outs', 'air_outs', 'runs', 'doubles', 'triples', 'home_runs', 'balls',
        'strikes', 'predicted_pitches'
    ]

    # Merge datasets
    merged_data = merge_datasets(training_data, fantasy_points_data)

    # Train the regression model
    model, scaler = train_regression_model(merged_data, metrics)

    # Extract weights
    weights = extract_weights(model, metrics)
    print('Optimized Weights:', weights)

    # Optionally, save the weights to a file
    with open('pitcher_optimized_weights.json', 'w') as f:
        import json
        json.dump(weights, f)

    print('Weights saved to optimized_weights.json')


if __name__ == "__main__":
    main()
