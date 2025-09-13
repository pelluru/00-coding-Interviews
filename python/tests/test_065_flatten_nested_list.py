import unittest
from problems.flatten_nested_list import flatten_nested_list
class TestFlattenNestedList (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(flatten_nested_list())
