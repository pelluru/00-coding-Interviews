package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem106SlidingWindowMedian

class Problem106SlidingWindowMedianSpec extends AnyFunSuite {
  test("sliding median") { 
    val r = Problem106SlidingWindowMedian.medianSlidingWindow(Array(1,3,-1,-3,5,3,6,7),3)
    assert(r.map(x => f"$x%.1f").toList == List("1.0","-1.0","-1.0","3.0","5.0","6.0"))
  }
}

