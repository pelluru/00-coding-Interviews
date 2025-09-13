
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem057IntervalMerge

class Problem057IntervalMergeSpec extends AnyFunSuite {
  test("merge intervals") {
    val r = Problem057IntervalMerge.merge(Array(Array(1,3),Array(2,6),Array(8,10),Array(15,18)))
    assert(r.length == 3)
  }
}

