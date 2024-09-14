import unittest
import pandas as pd
from baseball_stats.batter_data import process_batter_rows
from baseball_stats.pitch_data import process_pitcher_rows
from unittest.mock import patch
from baseball_stats.common_data import get_combined_data, is_barreled, is_contact, is_swing
from baseball_stats.queries import QueryBuilder


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


class TestCommonData(unittest.TestCase):

    @patch('baseball_stats.common_data.select_data')
    def test_get_combined_data(self, mock_select_data):
        mock_select_data.side_effect = [pd.DataFrame({'player_id': [1], 'hits': [10]}),
                                        pd.DataFrame({'player_id': [1], 'avg_ev': [5]})]
        query1 = QueryBuilder('SELECT player_id, hits FROM hitters')
        query1.empty = False
        query2 = QueryBuilder('SELECT player_id, avg_ev FROM all_plays')
        query2.empty = False
        result = get_combined_data(query1, query2, ('player_id',))
        self.assertEqual(result.shape, (1, 3))
        self.assertIn('hits', result.columns)
        self.assertIn('avg_ev', result.columns)

    def test_is_barreled(self):
        self.assertTrue(is_barreled(25, 100))
        self.assertFalse(is_barreled(10, 90))

    def test_is_contact(self):
        self.assertTrue(is_contact('Foul'))
        self.assertFalse(is_contact('Ball'))

    def test_is_swing(self):
        self.assertTrue(is_swing('Swinging Strike'))
        self.assertFalse(is_swing('Ball'))


if __name__ == '__main__':
    unittest.main()
