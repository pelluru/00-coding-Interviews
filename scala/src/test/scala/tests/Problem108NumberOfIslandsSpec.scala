package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem108NumberOfIslands

class Problem108NumberOfIslandsSpec extends AnyFunSuite {
  test("islands") {
    val g = Array(Array('1','1','0'), Array('0','1','0'), Array('1','0','1'))
    assert(Problem108NumberOfIslands.numIslands(g) == 3)
  }
}

