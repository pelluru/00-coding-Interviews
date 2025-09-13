package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem066FlattenDictKeys

class Problem066FlattenDictKeysSpec extends AnyFunSuite {
  test("flatten dict keys") { val r=Problem066FlattenDictKeys.flatten(Map("a"->Map("b"->1))); assert(r.contains("a.b")) }
}

