package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem038BinaryTreePreorder

class Problem038BinaryTreePreorderSpec extends AnyFunSuite {
  test("preorder") { val r=new Problem038BinaryTreePreorder.Node(2,new Problem038BinaryTreePreorder.Node(1,null,null),new Problem038BinaryTreePreorder.Node(3,null,null)); assert(Problem038BinaryTreePreorder.traverse(r) == List(2,1,3)) }
}

