import unittest
from problems.async_fetch_example import async_fetch_example
class TestAsyncFetchExample (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(async_fetch_example())
