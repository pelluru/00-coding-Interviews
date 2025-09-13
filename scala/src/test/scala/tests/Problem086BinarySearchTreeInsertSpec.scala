package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem086BinarySearchTreeInsert

class Problem086BinarySearchTreeInsertSpec extends AnyFunSuite {
  test("bst insert") { val r=Problem086BinarySearchTreeInsert.insert(null,2); Problem086BinarySearchTreeInsert.insert(r,1); Problem086BinarySearchTreeInsert.insert(r,3); assert(r.l.v==1 && r.r.v==3) }
}

