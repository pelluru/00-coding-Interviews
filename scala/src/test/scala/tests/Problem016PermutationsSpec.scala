package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem016Permutations

class Problem016PermutationsSpec extends AnyFunSuite {
  test("permute size") {
    assert(Problem016Permutations.permute(Array(1,2,3)).size === 6)
  }
}
