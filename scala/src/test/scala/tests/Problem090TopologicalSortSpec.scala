
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem090TopologicalSort

class Problem090TopologicalSortSpec extends AnyFunSuite {
  test("topo") {
    val out = Problem090TopologicalSort.topo(4, Array(Array(0,1),Array(0,2),Array(1,3),Array(2,3)))
    assert(out.size == 4)
  }
}

