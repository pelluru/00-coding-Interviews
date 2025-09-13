import unittest
from problems.flatten_dict_keys import flatten_dict_keys
class TestFlattenDictKeys (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(flatten_dict_keys())
