package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem047NQueensCount

class Problem047NQueensCountSpec extends AnyFunSuite {
  test("n-queens 4 -> 2") { assert(Problem047NQueensCount.count(4) == 2) }
}

