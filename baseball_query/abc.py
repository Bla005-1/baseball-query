import pandas as pd
from typing import *
from abc import ABC, abstractmethod

class DBMetric:
    def __init__(self, data: Dict):
        self.metric_name = data.get('metric_name')
        self.sql_value = data.get('sql_value', None)
        self.is_all_plays = data.get('is_all_plays', 0)
        self.is_totals_batter = data.get('is_totals_batter', 0)
        self.is_totals_pitcher = data.get('is_totals_pitcher', 0)
        self.is_totals_fielder = data.get('is_totals_fielder', 0)
        self.is_grouping = data.get('is_grouping', 0)
        self.is_python = data.get('is_python', 0)
        self.metric_description = data.get('metric_description')
        self.hidden = data.get('hidden', 0)
        self.dependencies = []
        dependencies = data.get('dependencies', '')
        if dependencies:
            self.dependencies = dependencies.split(',')

    def __repr__(self):
        return (
            f"Metric(metric_name='{self.metric_name}', sql_value='{self.sql_value}', "
            f"is_all_plays={self.is_all_plays}, is_totals_batter={self.is_totals_batter}, "
            f"is_totals_pitcher={self.is_totals_pitcher}, is_totals_fielder={self.is_totals_fielder}, "
            f"is_grouping={self.is_grouping}, metric_description='{self.metric_description}', "
            f"hidden={self.hidden}, dependencies='{self.dependencies}')"
        )

class BaseQueryBuilder(ABC):
    def __init__(self, player_type):
        self.player_type = player_type
        self.empty = False
        self.metric_names: List[str] = []
        self.group_columns: List[str] = []
        self.order_columns: List[str] = []
        self.python_metrics : List[str] = []

    @abstractmethod
    def set_table(self, table: str):
        pass

    @abstractmethod
    def get_query(self) -> str:
        pass

    @abstractmethod
    def get_args(self) -> List[str]:
        pass

    @abstractmethod
    def add_select(self, metric: DBMetric) -> Self:
        pass

    @abstractmethod
    def add_raw_where(self, where_clause: str, args: Optional[List[str] | str] = None) -> Self:
        pass

    @abstractmethod
    def group_by(self, column: str | List[str]) -> Self:
        pass

    @abstractmethod
    def order_by(self, column: str | List[str]) -> Self:
        pass

    @staticmethod
    def _parse_select(expression: str) -> Tuple:
        if ' AS ' in expression:
            clauses = expression.split(' AS ')
            alias = clauses[-1].strip()
        else:
            alias = expression
        return expression, alias

    @abstractmethod
    def get_where_clauses(self) -> List[str]:
        pass

    def get_group_columns(self) -> List[str]:
        return self.group_columns

    def get_metric_names(self) -> List[str]:
        return self.metric_names

    def get_order_columns(self) -> List[str]:
        return self.order_columns

    def __str__(self) -> str:
        return self.get_query()

    def __bool__(self) -> bool:
        self.get_query()
        return not self.empty

class BaseDBManager(ABC):
    @abstractmethod
    async def initialize_pool(self):
        pass

    @abstractmethod
    async def fetch_all(self, query: str, params: Optional[Tuple | Dict | List] = None) -> List[Dict]:
        pass

    @abstractmethod
    async def execute_update(self, query: str, params: Optional[Tuple | Dict | List] = None) -> int:
        pass

    @abstractmethod
    async def close(self):
        pass

    @abstractmethod
    async def get_column_values(self, query: str, column_name: str) -> List:
        pass

    @abstractmethod
    async def fetch_metric_sqls(self, metric_names: List[str]) -> Dict:
        pass

class BaseCache(ABC):
    cache: Any
    db_manager: BaseDBManager

    @abstractmethod
    def get_cache_entry(self, key: str) -> Any | None:
        pass

    @abstractmethod
    async def get_totals_batter(self) -> List[str]:
        pass

    @abstractmethod
    async def get_totals_pitcher(self) -> List[str]:
        pass

    @abstractmethod
    async def get_plays_metrics(self) -> List[str]:
        pass

    @abstractmethod
    async def get_group_metrics(self) -> List[str]:
        pass

    @abstractmethod
    async def get_metrics_dict(self) -> Dict[str, DBMetric]:
        pass

    @staticmethod
    def get_tables() -> Tuple[str, ...]:
        return 'league_averages', 'hitters', 'pitchers', 'fielders', 'all_plays'

    @abstractmethod
    async def get_table_columns_dict(self):
        pass


BuilderT = TypeVar('BuilderT', bound=BaseQueryBuilder)

class BaseQueryFactory(ABC):
    db_manager: BaseDBManager = None
    cache = None

    @abstractmethod
    async def initialize(self):
        pass

    @abstractmethod
    async def close(self):
        pass

    @abstractmethod
    async def create_query(self, metrics: List[str], player_type: str,
                           builder_cls: Optional[Type[BuilderT]] = None) -> BuilderT:
        pass

    @abstractmethod
    async def fetch_data(self, query_builder: BuilderT, skip_processor: bool = False) -> pd.DataFrame:
        pass

class VectorizedMetric(ABC):
    def __init__(self, names: str | List[str], dependencies: Tuple[str, ...]):
        if isinstance(names, str):
            names = [names]
        self.names = names
        self.dependencies = dependencies
        self.requires_row = False
        self.original_row = pd.Series()

    @abstractmethod
    def calculate(self, temp_df: pd.DataFrame) -> Dict:
        """Calculate the metric using vectorized operations."""
        pass

    def add_row(self, row: pd.Series):
        self.original_row = row


