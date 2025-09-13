import unittest
from problems.decorator_timing import decorator_timing
class TestDecoratorTiming (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(decorator_timing())
