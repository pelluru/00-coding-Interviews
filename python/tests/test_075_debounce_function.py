import unittest
from problems.debounce_function import debounce_function
class TestDebounceFunction (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(debounce_function())
