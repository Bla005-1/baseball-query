import unittest
from baseball_stats.baseball_query import BaseballQuery
from utils_for_test import *


class TestBaseballQuery(unittest.TestCase):

    def setUp(self):
        # Example setup for testing
        self.metric_keys = ['avg_ev', 'max_ev', 'zone_contact', 'hits', 'home_runs']
        self.player_type = 'batter'
        self.query = BaseballQuery(self.metric_keys, self.player_type)

    def test_initialization(self):
        # Test the initialization of BaseballQuery
        self.assertEqual(self.query.player_type, 'batter')
        self.assertEqual(self.query.groups, [])
        self.assertEqual(self.query.all_metrics, self.metric_keys)

    def test_add_where_and_group(self):
        # Test the add_where_and_group method
        self.query.add_where_and_group('name', 'Player Name')
        self.assertIn('name', self.query.groups)
        self.assertIn('Player Name', self.query.total_query.args)
        self.assertIn('Player Name', self.query.play_query.args)

    def test_extra_group(self):
        # Test the extra_group method
        self.query.add_group_column('team_name')
        self.assertIn('team_name', self.query.groups)

    def test_order_by(self):
        # Test the order_by method
        self.query.order_by('avg_ev')
        self.assertIn('avg_ev', self.query.play_query.order)
        self.assertIn('avg_ev', self.query.total_query.order)

    def test_str(self):
        # Test the __str__ method
        result = str(self.query)
        self.assertIsInstance(result, str)

    def test_single_metric_single_filter(self):
        query = BaseballQuery(['hits'], 'batter')
        query.add_filters({'year': '2023'})
        expected_sql = (
            'SELECT SUM(hits) AS hits FROM hitters  WHERE date LIKE ?'
        )
        self.assertEqual(expected_sql, query.total_query.finish_query())
        self.assertEqual(['2023%'], query.total_query.get_args())

    def test_multiple_metrics_multiple_filters(self):
        query = BaseballQuery(['hits', 'home_runs'], 'batter')
        query.add_filters({
            'year': '2023',
            'start_date': '2023-01-01',
            'end_date': '2023-12-31',
            'game_type': 'R'
        })
        expected_sql_totals = (
            'SELECT SUM(hits) AS hits, SUM(home_runs) AS home_runs FROM hitters  WHERE date LIKE ? '
            'AND date BETWEEN ? AND ? AND game_type = ?'
        )
        expected_args = ['2023%', '2023-01-01', '2023-12-31', 'R']
        actual_query = query.total_query.finish_query()
        compare_queries(self, expected_sql_totals, actual_query)
        self.assertEqual(expected_args, query.total_query.get_args())

    def test_play_query_with_filters(self):
        query = BaseballQuery(['contact_percent', 'zone_contact'], 'batter')
        query.add_filters({'year': '2022', 'game_type': 'R'})
        expected_sql_plays = (
            'SELECT GROUP_CONCAT(IFNULL(zone, 0)) as zones, GROUP_CONCAT(pitch_result) AS pitch_results FROM all_plays '
            ' WHERE date LIKE ? AND game_type = ?'
        )
        expected_args = ['2022%', 'R']
        actual_query = query.play_query.finish_query()
        compare_queries(self, expected_sql_plays, actual_query)
        self.assertEqual(expected_args, query.play_query.get_args())

    def test_query_with_grouping_and_order(self):
        query = BaseballQuery(['hits', 'home_runs'], 'batter')
        query.add_filters({'year': '2021'})
        query.add_group_column('team_name')
        query.order_by('hits')

        expected_sql_totals = (
            'SELECT SUM(hits) AS hits, SUM(home_runs) AS home_runs FROM hitters  WHERE date LIKE ? '
            'GROUP BY team_name ORDER BY hits'
        )
        expected_args = ['2021%']
        actual_query = query.total_query.finish_query()
        compare_queries(self, expected_sql_totals, actual_query)
        self.assertEqual(query.total_query.get_args(), expected_args)

    def test_combined_total_and_play_query(self):
        query = BaseballQuery(['hits', 'home_runs', 'team_name', 'max_velo'], 'batter')
        query.add_filters({
            'year': '2023',
            'game_type': 'R'
        })
        expected_sql_totals = (
            'SELECT SUM(hits) AS hits, SUM(home_runs) AS home_runs, team_name FROM hitters  WHERE date LIKE ? '
            'AND game_type = ? GROUP BY team_name'
        )
        expected_sql_plays = (
            'SELECT MAX(CAST(start_speed AS REAL)) AS max_velo, team_batting AS team_name FROM all_plays  '
            'WHERE date LIKE ? AND game_type = ? GROUP BY team_batting'
        )
        expected_args_totals = ['2023%', 'R']
        expected_args_plays = ['2023%', 'R']
        actual_totals = query.total_query.finish_query()
        actual_plays = query.play_query.finish_query()
        compare_queries(self, expected_sql_totals, actual_totals)
        compare_queries(self, expected_sql_plays, actual_plays)
        self.assertEqual(query.total_query.get_args(), expected_args_totals)
        self.assertEqual(query.play_query.get_args(), expected_args_plays)


if __name__ == '__main__':
    unittest.main()
