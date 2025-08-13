import pytest
from baseball_query.queries import SingleQueryBuilder, TotalsBuilder, PlaysBuilder
from baseball_query.sql_query import SQLQuery
from baseball_query.abc import DBMetric


@pytest.fixture
def simple_metric():
    return DBMetric({'metric_name': 'hits', 'sql_value': 'hits', 'is_totals_batter': 1})


def test_dynamic_where_single_value(simple_metric):
    b = SingleQueryBuilder('batter', SQLQuery())
    b.set_table('hitters')
    b.add_select(simple_metric)
    b.add_dynamic_where('season', '2023')
    assert b.get_query() == 'SELECT hits FROM hitters WHERE season = %s'
    assert b.get_args() == ['2023']


def test_dynamic_where_list_values(simple_metric):
    b = SingleQueryBuilder('batter', SQLQuery())
    b.set_table('hitters')
    b.add_select(simple_metric)
    b.add_dynamic_where('team', ['A', 'B'])
    query = 'SELECT hits FROM hitters WHERE team IN (%, s, %, s)'
    assert b.get_query() == query
    assert b.get_args() == ['A', 'B']


def test_dynamic_where_none_groups(simple_metric):
    b = SingleQueryBuilder('batter', SQLQuery())
    b.set_table('hitters')
    b.add_select(simple_metric)
    b.add_dynamic_where('team', [])
    assert 'GROUP BY team' in b.get_query()
    assert b.get_args() == []


def test_totals_builder_select_filtering():
    metric_ok = DBMetric({'metric_name': 'hits', 'sql_value': 'hits', 'is_totals_batter': 1})
    metric_bad = DBMetric({'metric_name': 'something', 'sql_value': 's', 'is_totals_pitcher': 1})
    b = TotalsBuilder('batter')
    b.add_select(metric_ok)
    b.add_select(metric_bad)
    q = b.get_query()
    assert q.startswith('SELECT hits FROM hitters')
    assert 'something' not in q


def test_plays_builder_select_filtering():
    metric_ok = DBMetric({'metric_name': 'play', 'sql_value': 'play', 'is_all_plays': 1})
    metric_bad = DBMetric({'metric_name': 'x', 'sql_value': 'x'})
    b = PlaysBuilder('batter')
    b.add_select(metric_ok)
    b.add_select(metric_bad)
    q = b.get_query()
    assert q.startswith('SELECT play FROM all_plays')
    assert ' x ' not in q
