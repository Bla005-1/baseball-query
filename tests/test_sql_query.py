import pytest
from baseball_query.sql_query import SQLQuery, BaseStrSQLQuery
from baseball_query.errors import EmptyQueryError


def test_sql_query_build_basic():
    q = SQLQuery()
    q.add_select('name').add_select('hits')
    q.set_from_table('hitters')
    q.add_where('season = %s')
    q.add_group_by('name').add_order_by('hits')
    query = q.build_query()
    expected = 'SELECT name, hits FROM hitters WHERE season = %s GROUP BY name ORDER BY hits'
    assert query == expected


def test_sql_query_missing_parts():
    q = SQLQuery()
    q.set_from_table('hitters')
    with pytest.raises(EmptyQueryError):
        q.build_query()


def test_base_str_sql_query_parsing():
    base = 'SELECT a, SUM(b) AS total_b FROM table1'
    q = BaseStrSQLQuery(base)
    assert q.select == ['a', 'SUM(b) AS total_b']
    assert q.from_table == 'table1'
    q.add_where('a = %s')
    q.add_group_by('a')
    built = q.build_query()
    expected = base + ' WHERE a = %s GROUP BY a'
    assert built == expected
