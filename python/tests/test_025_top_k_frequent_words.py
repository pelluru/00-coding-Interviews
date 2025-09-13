import unittest
from problems.top_k_frequent_words import top_k_frequent_words
class TestTopKFrequentWords (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(top_k_frequent_words())
