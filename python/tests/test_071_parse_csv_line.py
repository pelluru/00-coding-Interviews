import unittest
from problems.parse_csv_line import parse_csv_line
class TestParseCsvLine (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(parse_csv_line())
