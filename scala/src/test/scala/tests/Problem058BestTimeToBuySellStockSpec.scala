
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem058BestTimeToBuySellStock

class Problem058BestTimeToBuySellStockSpec extends AnyFunSuite {
  test("stock") {
    assert(Problem058BestTimeToBuySellStock.maxProfit(Array(7,1,5,3,6,4)) === 5)
  }
}

