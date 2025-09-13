package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem024MinHeapKSmallest

class Problem024MinHeapKSmallestSpec extends AnyFunSuite {
  test("k smallest") { assert(Problem024MinHeapKSmallest.kSmallest(Array(3,1,2,4),2).toList == List(1,2)) }
}

