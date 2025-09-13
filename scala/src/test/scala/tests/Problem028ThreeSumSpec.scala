package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem028ThreeSum

class Problem028ThreeSumSpec extends AnyFunSuite {
  test("3sum non-empty") {
    assert(Problem028ThreeSum.threeSum(Array(-1,0,1,2,-1,-4)).nonEmpty)
  }
}
