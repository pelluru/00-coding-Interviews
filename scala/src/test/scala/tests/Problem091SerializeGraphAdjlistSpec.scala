package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem091SerializeGraphAdjlist

class Problem091SerializeGraphAdjlistSpec extends AnyFunSuite {
  test("serialize graph") { val s=Problem091SerializeGraphAdjlist.serialize(Map(1->List(2,3),2->Nil)); assert(s.contains("1:2,3")) }
}

