import unittest
from problems.thread_safe_counter import thread_safe_counter
class TestThreadSafeCounter (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(thread_safe_counter())
