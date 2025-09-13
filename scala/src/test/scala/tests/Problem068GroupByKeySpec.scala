package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem068GroupByKey

class Problem068GroupByKeySpec extends AnyFunSuite {
  test("groupByKey") { val r=Problem068GroupByKey.groupByKey(List(("a",1),("a",2))); assert(r("a")==List(1,2)) }
}

