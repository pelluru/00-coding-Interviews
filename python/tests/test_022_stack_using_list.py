import unittest
from problems.stack_using_list import stack_using_list
class TestStackUsingList (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(stack_using_list())
