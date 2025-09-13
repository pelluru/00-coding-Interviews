import unittest
from problems.fib_memo import fib_memo
class TestFibMemo (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(fib_memo())
