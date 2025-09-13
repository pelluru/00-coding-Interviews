package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem033CoinChangeMin

class Problem033CoinChangeMinSpec extends AnyFunSuite {
  test("coin change") {
    assert(Problem033CoinChangeMin.coinChange(Array(1,2,5),11) === 3)
  }
}
