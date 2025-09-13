package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem065FlattenNestedList

class Problem065FlattenNestedListSpec extends AnyFunSuite {
  test("flatten nested") { assert(Problem065FlattenNestedList.flatten(List(1,List(2,List(3)),4)) == List(1,2,3,4)) }
}

