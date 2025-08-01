from baseball_query.utils import camel_to_snake, is_barreled, is_contact, is_swing
from baseball_query.static_data import CONTACT_RESULTS, ALL_SWINGS


def test_camel_to_snake():
    assert camel_to_snake('CamelCase') == 'camel_case'
    assert camel_to_snake('already_snake') == 'already_snake'


def test_is_barreled():
    # Perfect barrel should be True
    assert is_barreled(30, 110) is True
    # Clearly not a barrel
    assert is_barreled(60, 80) is False


def test_is_contact_and_swing():
    for result in CONTACT_RESULTS:
        assert is_contact(result) is True
    for result in ALL_SWINGS:
        assert is_swing(result) is True
    assert is_contact('Random') is False
    assert is_swing('Random') is False
