
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem044BfsGraph

class Problem044BfsGraphSpec extends AnyFunSuite {
  test("bfs") {
    val g = Map(1->List(2,3), 2->List(4), 3->Nil, 4->Nil)
    assert(Problem044BfsGraph.bfs(g,1) == List(1,2,3,4))
  }
}

