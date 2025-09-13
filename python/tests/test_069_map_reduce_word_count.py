import unittest
from problems.map_reduce_word_count import map_reduce_word_count
class TestMapReduceWordCount (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(map_reduce_word_count())
