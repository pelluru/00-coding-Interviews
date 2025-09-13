import unittest
from problems.context_manager_example import context_manager_example
class TestContextManagerExample (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(context_manager_example())
