package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem032UniquePathsGrid

class Problem032UniquePathsGridSpec extends AnyFunSuite {
  test("unique paths") {
    assert(Problem032UniquePathsGrid.uniquePaths(3,3) === 6)
  }
}
