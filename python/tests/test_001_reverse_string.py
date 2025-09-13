import unittest
from problems.reverse_string import reverse_string
class TestReverseString(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(reverse_string('abc'), 'cba')
