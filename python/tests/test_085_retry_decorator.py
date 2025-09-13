import unittest
from problems.retry_decorator import retry_decorator
class TestRetryDecorator (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(retry_decorator())
