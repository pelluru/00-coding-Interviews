import unittest
from problems.url_validation import url_validation
class TestUrlValidation (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(url_validation())
