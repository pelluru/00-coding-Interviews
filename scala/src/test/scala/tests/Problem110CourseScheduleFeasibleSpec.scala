package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem110CourseScheduleFeasible

class Problem110CourseScheduleFeasibleSpec extends AnyFunSuite {
  test("course schedule") {
    assert(Problem110CourseScheduleFeasible.canFinish(2, Array(Array(1,0))))
    assert(!Problem110CourseScheduleFeasible.canFinish(2, Array(Array(1,0),Array(0,1))))
  }
}

