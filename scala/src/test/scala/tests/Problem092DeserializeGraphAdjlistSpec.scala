package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem092DeserializeGraphAdjlist

class Problem092DeserializeGraphAdjlistSpec extends AnyFunSuite {
  test("deserialize graph") { val g=Problem092DeserializeGraphAdjlist.deserialize("1:2,3;2:"); assert(g(1)==List(2,3)) }
}

