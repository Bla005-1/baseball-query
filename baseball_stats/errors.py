
class NoDataFoundError(Exception):
    def __init__(self, message: str = 'No data found', query1=None, query2=None):
        self.message = message
        self.query1 = query1
        self.query2 = query2


class EmptyQueryError(Exception):
    def __init__(self, message: str = 'Both queries are empty'):
        self.message = message


class QueryExecutionError(Exception):
    def __init__(self, message: str = 'No data found', query1=None):
        self.message = message
        self.query1 = query1
