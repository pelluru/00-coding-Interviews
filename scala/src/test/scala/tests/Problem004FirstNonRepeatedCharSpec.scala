package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem004FirstNonRepeatedChar

class Problem004FirstNonRepeatedCharSpec extends AnyFunSuite {
  test("first non-repeated") {
    assert(Problem004FirstNonRepeatedChar.firstNonRepeated("aabbc").contains('c'))
  }
}

