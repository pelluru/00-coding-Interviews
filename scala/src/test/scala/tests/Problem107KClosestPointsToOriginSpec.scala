package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem107KClosestPointsToOrigin

class Problem107KClosestPointsToOriginSpec extends AnyFunSuite {
  test("k closest") {
    val r = Problem107KClosestPointsToOrigin.kClosest(Array(Array(1,3),Array(-2,2)), 1)
    assert(r.length == 1)
  }
}

