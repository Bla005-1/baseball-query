import unittest
from baseball_stats.db import daily_update


class TestDailyUpdate(unittest.TestCase):

    def test_daily_update(self):
        daily_update()


if __name__ == '__main__':
    unittest.main()
