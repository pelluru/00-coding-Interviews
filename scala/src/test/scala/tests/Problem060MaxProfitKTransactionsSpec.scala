package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem060MaxProfitKTransactions

class Problem060MaxProfitKTransactionsSpec extends AnyFunSuite {
  test("k transactions dp") { assert(Problem060MaxProfitKTransactions.maxProfit(2, Array(3,2,6,5,0,3)) == 7) }
}

