import unittest
from problems.design_cache_ttl import design_cache_ttl
class TestDesignCacheTtl (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(design_cache_ttl())
