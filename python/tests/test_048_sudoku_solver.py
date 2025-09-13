import unittest
from problems.sudoku_solver import sudoku_solver
class TestSudokuSolver (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(sudoku_solver())
