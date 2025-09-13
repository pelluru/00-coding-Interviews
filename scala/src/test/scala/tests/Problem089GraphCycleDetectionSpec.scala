package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem089GraphCycleDetection

class Problem089GraphCycleDetectionSpec extends AnyFunSuite {
  test("cycle detection") { assert(Problem089GraphCycleDetection.hasCycle(2, Array(Array(0,1),Array(1,0)))) }
}

