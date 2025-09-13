import unittest
from problems.lru_cache_simple import lru_cache_simple
class TestLruCacheSimple (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(lru_cache_simple())
