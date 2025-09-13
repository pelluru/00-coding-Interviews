package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem079PickleSerialize

class Problem079PickleSerializeSpec extends AnyFunSuite {
  test("serialize/deserialize") { val p=Problem079PickleSerialize.Person("a",1); assert(Problem079PickleSerialize.roundtrip(p)==p) }
}

