import unittest
from problems.transpose_matrix import transpose_matrix
class TestTransposeMatrix (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(transpose_matrix())
