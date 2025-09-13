package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem045DfsGraphRecursive

class Problem045DfsGraphRecursiveSpec extends AnyFunSuite {
  test("dfs") { val g=Map(1->List(2,3),2->List(4),3->Nil,4->Nil); assert(Problem045DfsGraphRecursive.dfs(g,1)==List(1,2,4,3)) }
}

