package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem080JsonSerializeCustom

class Problem080JsonSerializeCustomSpec extends AnyFunSuite {
  test("toJson") { val s=Problem080JsonSerializeCustom.toJson(Map("x"->1,"y"->"z")); assert(s.contains(""x":1") && s.contains(""y":"z"")) }
}

