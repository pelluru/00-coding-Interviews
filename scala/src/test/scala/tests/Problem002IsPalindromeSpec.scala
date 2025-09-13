package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem002IsPalindrome

class Problem002IsPalindromeSpec extends AnyFunSuite {
  test("examples") {
    assert(Problem002IsPalindrome.isPalindrome("racecar"))
    assert(!Problem002IsPalindrome.isPalindrome("hello"))
  }
}
