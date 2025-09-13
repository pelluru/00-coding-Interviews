package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem070TailFLIke

class Problem070TailFLIkeSpec extends AnyFunSuite {
  test("tailLike") { assert(Problem070TailFLIke.tailLike(List("1","2","3"),2) == List("2","3")) }
}

