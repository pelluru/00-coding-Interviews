package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem034WordBreak

class Problem034WordBreakSpec extends AnyFunSuite {
  test("word break") {
    assert(Problem034WordBreak.wordBreak("leetcode", Set("leet","code")))
  }
}
