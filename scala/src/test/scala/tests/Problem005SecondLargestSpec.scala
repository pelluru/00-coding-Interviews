package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem005SecondLargest

class Problem005SecondLargestSpec extends AnyFunSuite {
  test("second largest") {
    assert(Problem005SecondLargest.secondLargest(Array(1,3,2)).contains(2))
  }
}

