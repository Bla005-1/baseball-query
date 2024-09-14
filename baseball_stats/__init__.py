from .batter_data import *
from .pitch_data import *
from .common_data import get_combined_data
from .utils import create_league_average_table
from .queries import QueryBuilder, PlaysBuilder, TotalsBuilder
from .baseball_query import BaseballQuery
from .db import daily_update, create_table
from .static_data import *
from .builder_metrics import *
from .errors import *

create_table('all_plays', db_keys)
create_table('hitters', hitter_db_keys)
create_table('pitchers', pitcher_db_keys)
create_table('fielders', fielder_db_keys)
create_league_average_table()
