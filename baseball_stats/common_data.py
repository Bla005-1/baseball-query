import pandas as pd
from .utils import select_data
from .queries import QueryBuilder

all_swings = ['Foul', 'Foul Bunt', 'Foul Tip Bunt', 'Foul Pitchout', 'Missed Bunt', 'Foul Tip',
              'Swinging Strike', 'Swinging Strike (Blocked)', 'Swinging Pitchout']


def get_combined_data(query1: QueryBuilder, query2: QueryBuilder, merge_on=('name', 'player_id', 'league')
                      ) -> pd.DataFrame:
    if query1 and query2:
        data1 = select_data(query1.finish_query(), query1.get_args())
        data2 = select_data(query2.finish_query(), query2.get_args())
        df = pd.merge(pd.DataFrame(data1), pd.DataFrame(data2), on=merge_on)
    elif query1:
        data1 = select_data(query1.finish_query(), query1.get_args())
        df = pd.DataFrame(data1)
    elif query2:
        data2 = select_data(query2.finish_query(), query2.get_args())
        df = pd.DataFrame(data2)
    else:
        raise 'No metrics provided'
    return df


def is_barreled(launch_angle, exit_velocity):
    if exit_velocity < 98:
        return False
    elif exit_velocity == 98:
        return 26 <= launch_angle <= 30
    else:
        # Calculate expanded range
        if exit_velocity <= 116:
            additional_degrees = 3 + 2 * (exit_velocity - 100) if exit_velocity > 100 else 1 * (exit_velocity - 99)
            min_angle = 26 - additional_degrees
            max_angle = 30 + additional_degrees
        else:
            min_angle = 8
            max_angle = 50

        return min_angle <= launch_angle <= max_angle


def is_contact(pitch_r: str) -> bool:
    return 'play' in pitch_r or pitch_r == 'Foul' or pitch_r == 'Foul Bunt'


def is_swing(pitch_r: str) -> bool:
    return pitch_r in all_swings or 'play' in pitch_r
