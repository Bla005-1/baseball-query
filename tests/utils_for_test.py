import re


def extract_metrics(query):
    pattern = r'SELECT (.+?) FROM'
    match = re.search(pattern, query)
    if match:
        return match.group(1).split(', ')
    return []


def compare_queries(test_case, expected_sql, actual_query):

    expected_metrics = extract_metrics(expected_sql)
    actual_metrics = extract_metrics(actual_query)

    expected_remaining = re.sub(r'SELECT .+? FROM', 'SELECT ... FROM', expected_sql)
    actual_remaining = re.sub(r'SELECT .+? FROM', 'SELECT ... FROM', actual_query)
    test_case.assertEqual(expected_remaining, actual_remaining)

    test_case.assertTrue(all(metric in actual_metrics for metric in expected_metrics))
    test_case.assertTrue(all(metric in expected_metrics for metric in actual_metrics))
