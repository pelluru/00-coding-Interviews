package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem006RemoveDuplicates

class Problem006RemoveDuplicatesSpec extends AnyFunSuite {
  test("dedup") {
    assert(Problem006RemoveDuplicates.dedup(Array(1,2,1)).sameElements(Array(1,2)))
  }
}

