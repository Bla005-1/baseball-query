import numpy as np
import pandas as pd
from typing import *
from .static_data import ALL_SWINGS, CONTACT_RESULTS, BALL_RESULTS, SWINGING_STRIKE_RESULTS
from .metric_manager import VectorizedMetric
from .utils import is_barreled


def get_or_compute_mask(mask_name: str, temp_df: pd.DataFrame) -> pd.Series:
    if mask_name in temp_df:
        return temp_df[mask_name]
    else:
        if mask_name == 'is_swing':
            mask = temp_df['pitch_results'].isin(ALL_SWINGS)
        elif mask_name == 'is_contact':
            mask = temp_df['pitch_results'].isin(CONTACT_RESULTS)
        elif mask_name == 'is_ball':
            mask = temp_df['pitch_results'].isin(BALL_RESULTS)
        elif mask_name == 'is_swinging_strike':
            mask = temp_df['pitch_results'].isin(SWINGING_STRIKE_RESULTS)
        else:
            raise ValueError(f'Invalid mask name: {mask_name}')
        temp_df[mask_name] = mask
        return mask


class ContactPercent(VectorizedMetric):
    def __init__(self):
        super().__init__(['contact_percent', 'whiff_percent'], dependencies=('pitch_results',))

    def calculate(self, temp_df: pd.DataFrame) -> dict:
        is_swing_mask = get_or_compute_mask('is_swing', temp_df)
        is_contact_mask = get_or_compute_mask('is_contact', temp_df)

        total_swings = is_swing_mask.sum()
        total_contacts = is_contact_mask.sum()

        contact_percent = (total_contacts / total_swings) * 100 if total_swings else 0
        whiff_percent = ((total_swings - total_contacts) / total_swings) * 100 if total_swings else 0
        return {'contact_percent': round(contact_percent, 2), 'whiff_percent': round(whiff_percent, 2)}


class ZoneContact(VectorizedMetric):
    def __init__(self):
        super().__init__(['zone_contact', 'zone_swing_percent', 'zone_percent'],
                         dependencies=('pitch_results', 'zones'))

    def calculate(self, temp_df: pd.DataFrame) -> dict:
        in_zone_mask = (temp_df['zones'] > 0) & (temp_df['zones'] < 9)
        is_contact_mask = get_or_compute_mask('is_contact', temp_df)
        total_swings = get_or_compute_mask('is_swing', temp_df)
        in_zone = in_zone_mask.sum()
        in_zone_contact = (in_zone_mask & is_contact_mask).sum()
        in_zone_swings = (in_zone_mask & total_swings).sum()
        all_pitches = len(temp_df)
        zone_swing_percent = in_zone_swings / in_zone * 100 if in_zone else 0
        zone_contact = (in_zone_contact / in_zone_swings) * 100 if in_zone_swings else 0
        zone_percent = in_zone / all_pitches * 100 if all_pitches else 0
        return {'zone_contact': round(zone_contact, 2), 'zone_swing_percent': round(zone_swing_percent, 2),
                'zone_percent': round(zone_percent, 2)}


class SwingPercent(VectorizedMetric):
    def __init__(self):
        super().__init__('swing_percent', dependencies=('pitch_results',))

    def calculate(self, temp_df: pd.DataFrame) -> dict:
        is_swing_mask = get_or_compute_mask('is_swing', temp_df)

        total_swings = is_swing_mask.sum()
        all_pitches = len(temp_df)

        swing_percent = (total_swings / all_pitches) * 100 if all_pitches else 0
        return {'swing_percent': round(swing_percent, 2)}


class ChasePercent(VectorizedMetric):
    def __init__(self):
        super().__init__('chase_percent', dependencies=('pitch_results', 'zones'))

    def calculate(self, temp_df: pd.DataFrame) -> dict:
        out_of_zone_mask = temp_df['zones'] > 9
        is_swing_mask = get_or_compute_mask('is_swing', temp_df)

        out_of_zone_swings = (out_of_zone_mask & is_swing_mask).sum()
        out_of_zone = out_of_zone_mask.sum()

        chase_percent = (out_of_zone_swings / out_of_zone) * 100 if out_of_zone else 0
        return {'chase_percent': round(chase_percent, 2)}


class BallStrikePercent(VectorizedMetric):
    def __init__(self):
        super().__init__(['ball_percent', 'strike_percent'], dependencies=('pitch_results',))

    def calculate(self, temp_df: pd.DataFrame) -> dict:
        balls_mask = get_or_compute_mask('is_ball', temp_df)
        balls = balls_mask.sum()
        all_pitches = len(temp_df)
        ball_percent = balls / all_pitches if all_pitches else 0
        strike_percent = (all_pitches - balls) / all_pitches if all_pitches else 0
        return {'ball_percent': round(ball_percent * 100, 2), 'strike_percent': round(strike_percent * 100, 2)}


class StrikeMetrics(VectorizedMetric):
    def __init__(self):
        super().__init__(['swstr_percent', 'csw_percent'], dependencies=('pitch_results',))

    def calculate(self, temp_df: pd.DataFrame) -> dict:
        swinging_strikes_mask = get_or_compute_mask('is_swinging_strike', temp_df)
        strikes_mask = temp_df['pitch_results'].isin(['Called Strike'])

        swinging_strikes = swinging_strikes_mask.sum()
        strikes = strikes_mask.sum()
        all_pitches = len(temp_df)
        csw_percent = ((strikes + swinging_strikes) / all_pitches) * 100 if all_pitches else 0
        swstr_percent = swinging_strikes / all_pitches * 100 if all_pitches else 0

        return {
            'swstr_percent': round(swstr_percent, 2),
            'csw_percent': round(csw_percent, 2)
        }


class FlyBallPercentage(VectorizedMetric):
    def __init__(self):
        super().__init__('FB_percent', dependencies=('trajectories',))

    def calculate(self, temp_df: pd.DataFrame) -> dict:
        fly_ball_mask: pd.Series[bool] = temp_df['trajectories'] == 'fly_ball'  # type: ignore
        fly_balls = fly_ball_mask.sum()
        batted_ball_mask: pd.Series[bool] = temp_df['trajectories'].notna()  # type: ignore
        all_hits = batted_ball_mask.sum()
        fb_percent = fly_balls / all_hits if all_hits else 0
        return {'FB_percent': round(fb_percent * 100, 2)}


class GroundBallPercentage(VectorizedMetric):
    def __init__(self):
        super().__init__('GB_percent', dependencies=('trajectories',))

    def calculate(self, temp_df: pd.DataFrame) -> dict:
        ground_ball_mask: pd.Series[bool] = temp_df['trajectories'] == 'ground_ball'  # type: ignore
        ground_balls = ground_ball_mask.sum()
        batted_ball_mask: pd.Series[bool] = temp_df['trajectories'].notna()  # type: ignore
        all_hits = batted_ball_mask.sum()
        gb_percent = ground_balls / all_hits if all_hits else 0
        return {'GB_percent': round(gb_percent * 100, 2)}


class LineDrivePercentage(VectorizedMetric):
    def __init__(self):
        super().__init__('LD_percent', dependencies=('trajectories',))

    def calculate(self, temp_df: pd.DataFrame) -> dict:
        line_drive_mask: pd.Series[bool] = temp_df['trajectories'] == 'line_drive'  # type: ignore
        line_drives = line_drive_mask.sum()
        batted_ball_mask: pd.Series[bool] = temp_df['trajectories'].notna()  # type: ignore
        all_hits = batted_ball_mask.sum()
        ld_percent = line_drives / all_hits if all_hits else 0
        return {'LD_percent': round(ld_percent * 100, 2)}


class AverageExitVelocityOnFlyBalls(VectorizedMetric):
    def __init__(self):
        super().__init__('avg_ev_on_FB', dependencies=('trajectories', 'hit_speeds'))

    def calculate(self, temp_df: pd.DataFrame) -> dict:
        fly_ball_mask: pd.Series[bool] = (temp_df['trajectories'] == 'fly_ball') & (temp_df['hit_speeds'].notna())  # type: ignore
        fly_balls = fly_ball_mask.sum()
        fb_ev = temp_df.loc[fly_ball_mask, 'hit_speeds'].sum()
        avg_ev_on_fb = fb_ev / fly_balls if fly_balls else 0
        return {'avg_ev_on_FB': round(avg_ev_on_fb, 2)}


class AverageExitVelocityOnLineDrives(VectorizedMetric):
    def __init__(self):
        super().__init__('avg_ev_on_LD', dependencies=('trajectories', 'hit_speeds'))

    def calculate(self, temp_df: pd.DataFrame) -> dict:
        line_drive_mask: pd.Series[bool] = (temp_df['trajectories'] == 'line_drive') & (temp_df['hit_speeds'].notna())  # type: ignore
        line_drives = line_drive_mask.sum()
        ld_ev = temp_df.loc[line_drive_mask, 'hit_speeds'].sum()
        avg_ev_on_ld = ld_ev / line_drives if line_drives else 0
        return {'avg_ev_on_LD': round(avg_ev_on_ld, 2)}


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


class BarrelPercent(VectorizedMetric):
    def __init__(self):
        super().__init__('barrel_per_bbe', dependencies=('hit_speeds', 'launch_angles'))

    def calculate(self, temp_df: pd.DataFrame) -> dict:
        valid_rows = temp_df.dropna(subset=['launch_angles', 'hit_speeds'])
        barreled_mask = valid_rows.apply(
            lambda row: is_barreled(row['launch_angles'], row['hit_speeds']), axis=1
        )
        total_barrels = barreled_mask.sum()
        total_events = len(valid_rows)

        barrel_per_bbe = (total_barrels / total_events) * 100 if total_events else 0

        return {'barrel_per_bbe': round(barrel_per_bbe, 2)}


class Percentile90(VectorizedMetric):
    def __init__(self):
        super().__init__('percentile_90', dependencies=('hit_speeds',))

    def calculate(self, temp_df: pd.DataFrame) -> dict:
        hit_speeds = temp_df['hit_speeds'].dropna()
        percentile_90 = np.percentile(hit_speeds, 90) if not hit_speeds.empty else 0

        return {'percentile_90': percentile_90}


COMPLEX_METRICS_DICT = {
    'contact_percent': ContactPercent,
    'whiff_percent': ContactPercent,
    'zone_contact': ZoneContact,
    'chase_percent': ChasePercent,
    'swing_percent': SwingPercent,
    'zone_swing_percent': ZoneContact,
    'strike_percent': BallStrikePercent,
    'csw_percent': StrikeMetrics,
    'swstr_percent': StrikeMetrics,
    'ball_percent': BallStrikePercent,
    'zone_percent': ZoneContact,
    'FB_percent': FlyBallPercentage,
    'GB_percent': GroundBallPercentage,
    'LD_percent': LineDrivePercentage,
    'avg_ev_on_FB': AverageExitVelocityOnFlyBalls,
    'avg_ev_on_LD': AverageExitVelocityOnLineDrives,
    'pulled_FB_percent': PulledFB,
    'avg_ev_on_pulled_FB': PulledFB,
    'xwOBA': ExpectedWeightedOBA,
    'xwOBAcon': ExpectedWeightedOBA,
    'barrel_per_bbe': BarrelPercent,
    'percentile_90': Percentile90
}
