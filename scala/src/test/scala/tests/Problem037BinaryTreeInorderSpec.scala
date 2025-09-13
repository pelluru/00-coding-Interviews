package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem037BinaryTreeInorder

class Problem037BinaryTreeInorderSpec extends AnyFunSuite {
  test("inorder") { val r=new Problem037BinaryTreeInorder.Node(2,new Problem037BinaryTreeInorder.Node(1,null,null),new Problem037BinaryTreeInorder.Node(3,null,null)); assert(Problem037BinaryTreeInorder.traverse(r) == List(1,2,3)) }
}

