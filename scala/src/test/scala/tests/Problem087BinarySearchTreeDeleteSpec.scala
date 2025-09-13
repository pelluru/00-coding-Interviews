package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem087BinarySearchTreeDelete

class Problem087BinarySearchTreeDeleteSpec extends AnyFunSuite {
  test("bst delete") { val r=new Problem087BinarySearchTreeDelete.Node(2,new Problem087BinarySearchTreeDelete.Node(1,null,null), new Problem087BinarySearchTreeDelete.Node(3,null,null)); val d=Problem087BinarySearchTreeDelete.delete(r,3); assert(d.r==null) }
}

