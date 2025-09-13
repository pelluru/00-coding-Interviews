package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem048SudokuSolver

class Problem048SudokuSolverSpec extends AnyFunSuite {
  test("sudoku solvable") { val b=Array.fill(9)(Array.fill(9)('.')); assert(Problem048SudokuSolver.solve(b)) }
}

