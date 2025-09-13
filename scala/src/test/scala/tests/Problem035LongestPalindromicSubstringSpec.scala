package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem035LongestPalindromicSubstring

class Problem035LongestPalindromicSubstringSpec extends AnyFunSuite {
  test("lps") {
    val r = Problem035LongestPalindromicSubstring.lps("babad")
    assert(r == "bab" || r == "aba")
  }
}
