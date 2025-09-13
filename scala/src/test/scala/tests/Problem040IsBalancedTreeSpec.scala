
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem040IsBalancedTree

class Problem040IsBalancedTreeSpec extends AnyFunSuite {
  test("balanced") {
    val r = new Problem040IsBalancedTree.Node(1, new Problem040IsBalancedTree.Node(2,null,null), null)
    assert(Problem040IsBalancedTree.isBalanced(r))
  }
}

