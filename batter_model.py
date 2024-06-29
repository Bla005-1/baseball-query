import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from utils import select_data
from batter_data import get_batter_data, basic_batt_calcs


draft_kings_points = {
    'R': 2.0,
    'H': 3.0,
    '2B': 2.0,
    '3B': 5.0,
    'HR': 7.0,
    'RBI': 2.0,
    'BB': 2.0,
    'SB': 5.0,
    'HBP': 2.0
}


# Load player metrics data (training data)
def load_training_data():
    league = 'MLB'
    names = select_data('SELECT DISTINCT name FROM hitters WHERE game_type = "R" AND league = "MLB"')
    names = [r['name'] for r in names]
    training_data = pd.DataFrame(get_batter_data(names, league=league))
    extra_training_data = pd.DataFrame(basic_batt_calcs(names, league=league))
    merged_data = pd.merge(training_data, extra_training_data, left_on='batter_name', right_on='name', how='inner')
    return merged_data.dropna()


# Load fantasy points data (actual results)
def load_fantasy_points_data():
    with open('fantrax_data.txt', 'r') as f:
        data_str = f.read()
    lines = data_str.strip().split('\n')
    headers = lines[0].split('\t')
    data = []
    for line in lines[1:]:
        values = line.split('\t')
        values[1] = values[1].split(',')[0]
        entry = dict(zip(headers, values))
        data.append(entry)
    return pd.DataFrame(data)


# Merge the datasets on player's name
def merge_datasets(metrics_data, fantasy_points_data):
    # Convert names to lower case to ensure proper merging
    metrics_data['batter_name'] = metrics_data['batter_name'].str.lower()
    fantasy_points_data['Name'] = fantasy_points_data['Name'].str.lower()
    merged_data = pd.merge(metrics_data, fantasy_points_data[['Name', 'FPts']], left_on='batter_name', right_on='Name')
    return merged_data


# Train a regression model to learn the relationship between metrics and fantasy points
def train_regression_model(data, metrics):
    x = data[metrics].astype(float)
    y = data['FPts'].astype(float)

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

    # Load data
    training_data = load_training_data()
    fantasy_points_data = load_fantasy_points_data()

    # Define relevant metrics
    metrics = [
        'avg_ev', 'max_ev', 'barrel_per_bbe', 'contact_percent', 'zone_contact', 'chase_percent', 'swing_percent',
        'zone_swing_percent', 'avg_hit_angle', 'percentile_90', 'RBI', 'OBP', 'SLG', 'SB', 'G', 'pitches'
    ]

    # Merge datasets
    merged_data = merge_datasets(training_data, fantasy_points_data)

    # Train the regression model
    model, scaler = train_regression_model(merged_data, metrics)

    # Extract weights
    weights = extract_weights(model, metrics)
    print('Optimized Weights:', weights)

    # Optionally, save the weights to a file
    with open('batter_optimized_weights.json', 'w') as f:
        import json
        json.dump(weights, f)

    print('Weights saved to optimized_weights.json')


if __name__ == "__main__":
    main()
