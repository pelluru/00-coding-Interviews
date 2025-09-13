package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem109WordLadderShortest

class Problem109WordLadderShortestSpec extends AnyFunSuite {
  test("word ladder") {
    val length = Problem109WordLadderShortest.ladderLength("hit","cog", List("hot","dot","dog","lot","log","cog"))
    assert(length == 5)
  }
}

