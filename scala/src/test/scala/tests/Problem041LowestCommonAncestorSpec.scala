
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem041LowestCommonAncestor

class Problem041LowestCommonAncestorSpec extends AnyFunSuite {
  test("lca") {
    val r = new Problem041LowestCommonAncestor.Node(3, null, null)
    r.l = new Problem041LowestCommonAncestor.Node(5, null, null); r.r = new Problem041LowestCommonAncestor.Node(1, null, null)
    assert(Problem041LowestCommonAncestor.lca(r, r.l, r.r).v == 3)
  }
}

