
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem043DijkstraSimple

class Problem043DijkstraSimpleSpec extends AnyFunSuite {
  test("dijkstra") {
    val g = Map("A" -> List(("B",1)), "B" -> List(("C",2)), "C" -> Nil)
    assert(Problem043DijkstraSimple.dijkstra(g,"A")("C") === 3)
  }
}

