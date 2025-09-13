package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem001ReverseString

class Problem001ReverseStringSpec extends AnyFunSuite {
  test("basic") {
    assert(Problem001ReverseString.reverseString("abc") === "cba")
    assert(Problem001ReverseString.reverseString("") === "")
    assert(Problem001ReverseString.reverseString(null) == null)
  }
}
