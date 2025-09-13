import unittest
from problems.interval_merge import interval_merge
class TestIntervalMerge (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(interval_merge())
