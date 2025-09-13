package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem039BinaryTreePostorder

class Problem039BinaryTreePostorderSpec extends AnyFunSuite {
  test("postorder") { val r=new Problem039BinaryTreePostorder.Node(2,new Problem039BinaryTreePostorder.Node(1,null,null),new Problem039BinaryTreePostorder.Node(3,null,null)); assert(Problem039BinaryTreePostorder.traverse(r) == List(1,3,2)) }
}

