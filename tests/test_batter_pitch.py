import unittest
import pandas as pd
from baseball_stats.batter_data import process_batter_rows
from baseball_stats.pitch_data import process_pitcher_rows


class TestBatterData(unittest.TestCase):

    def test_process_batter_rows(self):
        df = pd.DataFrame({
            'OBP': [0.4, 0.35],
            'SLG': [0.6, 0.5],
            'barrel_per_bbe': [0.1, 0.15]
        })
        metrics = ['OBS', 'SLG']
        processed_df = process_batter_rows(df, metrics)
        self.assertIn('OBS', processed_df.columns)
        self.assertEqual(processed_df['OBS'].tolist(), [1.0, 0.85])


class TestPitcherData(unittest.TestCase):

    def test_process_pitcher_rows(self):
        df = pd.DataFrame({
            'pitch_results': ['Foul', 'Ball', 'Ball', 'Ball', 'Called Strike'],
        })
        metrics = ['strike_percent', 'ball_percent']
        processed_df = process_pitcher_rows(df, metrics)
        self.assertIn('strike_percent', processed_df.columns)
        self.assertIn('ball_percent', processed_df.columns)



if __name__ == '__main__':
    unittest.main()
