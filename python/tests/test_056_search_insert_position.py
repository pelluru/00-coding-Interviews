import unittest
from problems.search_insert_position import search_insert_position
class TestSearchInsertPosition (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(search_insert_position())
