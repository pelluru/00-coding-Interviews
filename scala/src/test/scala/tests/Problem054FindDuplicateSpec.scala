
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem054FindDuplicate

class Problem054FindDuplicateSpec extends AnyFunSuite {
  test("dup") {
    assert(Problem054FindDuplicate.findDuplicate(Array(1,3,4,2,2)) === 2)
  }
}

