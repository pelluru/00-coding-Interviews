import unittest
from problems.matrix_multiply import matrix_multiply
class TestMatrixMultiply (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(matrix_multiply())
