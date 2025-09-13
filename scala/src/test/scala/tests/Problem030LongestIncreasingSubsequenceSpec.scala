package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem030LongestIncreasingSubsequence

class Problem030LongestIncreasingSubsequenceSpec extends AnyFunSuite {
  test("lis") {
    assert(Problem030LongestIncreasingSubsequence.lis(Array(10,9,2,5,3,7,101,18)) === 4)
  }
}
