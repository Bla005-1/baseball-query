

class BaseballStatsError(Exception):
    """Base exception for baseball query errors."""

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class NoDataFoundError(BaseballStatsError):
    """Raised when a query returns no data."""

    def __init__(self, message: str = 'No data found', query1=None, query2=None):
        super().__init__(message)
        self.query1 = query1
        self.query2 = query2


class EmptyQueryError(BaseballStatsError):
    """Raised when a SQL query is built without any clauses."""

    def __init__(self, message: str = 'While building the query, it was empty'):
        super().__init__(message)


class QueryExecutionError(BaseballStatsError):
    """Indicates failure while executing a SQL query."""

    def __init__(self, message: str = 'Error while executing a query', query1=None):
        super().__init__(message)
        self.query1 = query1

    def __str__(self):
        return self.message + ' ' + self.query1
