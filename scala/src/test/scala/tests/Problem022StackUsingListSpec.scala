package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem022StackUsingList

class Problem022StackUsingListSpec extends AnyFunSuite {
  test("stack") { val s=new Problem022StackUsingList.StackX[Int]; s.push(1); s.push(2); assert(s.pop()==2) }
}

