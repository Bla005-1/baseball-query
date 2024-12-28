import re
from .static_data import ALL_SWINGS, CONTACT_RESULTS

def camel_to_snake(camel_case: str) -> str:
    return re.sub(r'(?<!^)(?=[A-Z])', '_', camel_case).lower()


def is_barreled(launch_angle: int | float, exit_velocity: int | float) -> bool:
    # Perfect Barrel:
    # - (speed * 1.5 - angle) >= 129
    # - (speed + angle * 2) >= 156
    # - speed >= 106
    # - angle between 4 and 48
    if (
        (exit_velocity * 1.5 - launch_angle) >= 129
        and (exit_velocity + launch_angle * 2) >= 156
        and exit_velocity >= 106
        and 4 <= launch_angle <= 48
    ):
        return True

    # Barrel:
    # - (speed * 1.5 - angle) >= 117
    # - (speed + angle) >= 124
    # - speed >= 98
    # - angle between 4 and 50
    elif (
        (exit_velocity * 1.5 - launch_angle) >= 117
        and (exit_velocity + launch_angle) >= 124
        and exit_velocity >= 98
        and 4 <= launch_angle <= 50
    ):
        return True

    # Near-Barrel:
    # - (speed * 1.5 - angle) >= 111
    # - (speed + angle) >= 119
    # - speed >= 95
    # - angle between 0 and 52
    elif (
        (exit_velocity * 1.5 - launch_angle) >= 111
        and (exit_velocity + launch_angle) >= 119
        and exit_velocity >= 95
        and 0 <= launch_angle <= 52
    ):
        return True

    # Not Barreled
    return False


def is_contact(pitch_r: str) -> bool:
    return pitch_r in CONTACT_RESULTS


def is_swing(pitch_r: str) -> bool:
    return pitch_r in ALL_SWINGS