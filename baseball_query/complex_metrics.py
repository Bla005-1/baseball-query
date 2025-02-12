import numpy as np
import pandas as pd
from typing import *
from .abc import VectorizedMetric


class PulledFB(VectorizedMetric):
    def __init__(self):
        super().__init__(['pulled_FB_percent', 'avg_ev_on_pulled_FB'],
                         dependencies=('trajectories', 'hit_speeds', 'hit_coordinates', 'bat_sides'))

    def calculate(self, temp_df: pd.DataFrame) -> dict:
        # spray_angle = -arctan((hc_x - 130) / (213 - hc_y)) + pi / 2
        hc_x = temp_df['hit_coordinates'].apply(lambda coord: coord[0])
        hc_y = temp_df['hit_coordinates'].apply(lambda coord: coord[1])
        home_x, home_y = 130, 213

        # Spray angle calculation with home plate adjustment
        spray_angle = -np.arctan2((home_y - hc_y), (hc_x - home_x)) + np.pi / 2
        spray_angle_deg = np.degrees(spray_angle)
        right_handed = (temp_df['bat_sides'] == 'R') & (spray_angle_deg >= -45) & (spray_angle_deg <= -5)
        left_handed = (temp_df['bat_sides'] == 'L') & (spray_angle_deg >= 5) & (spray_angle_deg <= 45)
        pulled_fb_mask = (temp_df['trajectories'] == 'fly_ball') & (right_handed | left_handed)

        total_fly_balls = (temp_df['trajectories'] == 'fly_ball').sum()  # type: ignore
        pulled_fly_balls = pulled_fb_mask.sum()
        pulled_fb_ev = temp_df.loc[pulled_fb_mask, 'hit_speeds'].sum()
        pulled_fb_percent = (pulled_fly_balls / total_fly_balls) * 100 if total_fly_balls else 0
        avg_ev_on_pulled_fb = pulled_fb_ev / pulled_fly_balls if pulled_fly_balls else 0

        return {
            'pulled_FB_percent': round(pulled_fb_percent, 2),
            'avg_ev_on_pulled_FB': round(avg_ev_on_pulled_fb, 2)
        }


class ExpectedWeightedOBA(VectorizedMetric):
    def __init__(self, probabilities: List[Dict] = None):
        super().__init__(['xwOBA', 'xwOBAcon'],
                         dependencies=('hit_speeds', 'launch_angles'))
        self.requires_row = True
        self.probabilities = probabilities

    def calculate(self, temp_df: pd.DataFrame) -> dict:
        probabilities = pd.DataFrame(self.probabilities)
        ev_la_pairs = [
            (int((ev // 2) * 2), int((la // 3) * 3))
            for ev, la in zip(temp_df['hit_speeds'], temp_df['launch_angles'])
            if not (np.isnan(ev) or np.isnan(la))
        ]
        ev_la_pairs_df = pd.DataFrame(ev_la_pairs, columns=['ev_bin', 'la_bin'])
        ev_la_pair_counts = ev_la_pairs_df.value_counts().reset_index(name='frequency')

        # Step 2: Merge ev_la_pair_counts with probabilities for matching pairs
        matched_probs = probabilities.merge(ev_la_pair_counts, on=['ev_bin', 'la_bin'], how='inner')

        # Step 3: Compute adjusted probabilities directly during the merge
        matched_probs['prob_single'] *= matched_probs['frequency']
        matched_probs['prob_double'] *= matched_probs['frequency']
        matched_probs['prob_triple'] *= matched_probs['frequency']
        matched_probs['prob_home_run'] *= matched_probs['frequency']
        # Sum probabilities for relevant outcomes
        prob_single = matched_probs['prob_single'].sum()
        prob_double = matched_probs['prob_double'].sum()
        prob_triple = matched_probs['prob_triple'].sum()
        prob_home_run = matched_probs['prob_home_run'].sum()

        base_on_balls = float(self.original_row['base_on_balls'])
        intentional_walks = float(self.original_row['intentional_walks'])
        un_intentional_walks = base_on_balls - intentional_walks
        hit_by_pitch = float(self.original_row['hit_by_pitch'])
        at_bats = float(self.original_row['at_bats'])
        sac_flies = float(self.original_row['sac_flies'])
        batted_ball_events = len(ev_la_pairs)

        # Weights for xwOBA and xwOBACON
        w_1b, w_2b, w_3b, w_hr, w_bb, w_hbp = 0.882, 1.254, 1.59, 2.05, 0.689, 0.72

        xw_oba = (((w_1b * prob_single) +
                 (w_2b * prob_double) +
                 (w_3b * prob_triple) +
                 (w_hr * prob_home_run) +
                 (w_bb * un_intentional_walks) +
                 (w_hbp * hit_by_pitch)) /
                 (at_bats + un_intentional_walks + sac_flies + hit_by_pitch)) if (
                 (at_bats + un_intentional_walks + sac_flies + hit_by_pitch) > 0) else 0

        xw_oba_con = ((w_1b * prob_single) +
                    (w_2b * prob_double) +
                    (w_3b * prob_triple) +
                    (w_hr * prob_home_run)) / batted_ball_events if batted_ball_events > 0 else 0

        return {
                'xwOBA': xw_oba,
                'xwOBAcon': xw_oba_con
            }


class Percentile90(VectorizedMetric):
    def __init__(self):
        super().__init__('percentile_90', dependencies=('hit_speeds',))

    def calculate(self, temp_df: pd.DataFrame) -> dict:
        hit_speeds = temp_df['hit_speeds'].dropna()
        percentile_90 = np.percentile(hit_speeds, 90) if not hit_speeds.empty else 0

        return {'percentile_90': percentile_90}


COMPLEX_METRICS_DICT = {
    'pulled_FB_percent': PulledFB,
    'avg_ev_on_pulled_FB': PulledFB,
    'xwOBA': ExpectedWeightedOBA,
    'xwOBAcon': ExpectedWeightedOBA,
    'percentile_90': Percentile90
}
