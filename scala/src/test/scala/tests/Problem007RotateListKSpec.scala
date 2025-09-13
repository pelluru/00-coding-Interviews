package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem007RotateListK

class Problem007RotateListKSpec extends AnyFunSuite {
  test("rotate") {
    assert(Problem007RotateListK.rotateRight(Array(1,2,3,4),1).sameElements(Array(4,1,2,3)))
  }
}

