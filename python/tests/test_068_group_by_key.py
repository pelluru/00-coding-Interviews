import unittest
from problems.group_by_key import group_by_key
class TestGroupByKey (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(group_by_key())
