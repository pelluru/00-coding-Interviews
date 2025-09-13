import unittest
from problems.edit_distance import edit_distance
class TestEditDistance (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(edit_distance())
