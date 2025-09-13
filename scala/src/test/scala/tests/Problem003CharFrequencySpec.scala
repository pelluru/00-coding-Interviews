package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem003CharFrequency

class Problem003CharFrequencySpec extends AnyFunSuite {
  test("freq") {
    val m = Problem003CharFrequency.charFrequency("aab")
    assert(m('a') === 2)
    assert(m('b') === 1)
  }
}
