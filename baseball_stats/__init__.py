from .batter_data import get_batter_data, process_batter_rows
from .pitch_data import get_pitcher_data, process_pitcher_rows
from .common_data import get_combined_data
from .utils import create_league_average_table, QueryBuilder, PlaysBuilder, TotalsBuilder
from .db import daily_update, create_table
from .static_data import *

create_table('all_plays', db_keys)
create_table('hitters', hitter_db_keys)
create_table('pitchers', pitcher_db_keys)
create_table('fielders', fielder_db_keys)
create_league_average_table()
