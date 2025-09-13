
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem056SearchInsertPosition

class Problem056SearchInsertPositionSpec extends AnyFunSuite {
  test("search insert") {
    assert(Problem056SearchInsertPosition.searchInsert(Array(1,2,4,5),3) === 2)
  }
}

