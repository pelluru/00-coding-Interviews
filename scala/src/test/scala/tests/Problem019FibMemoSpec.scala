package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem019FibMemo

class Problem019FibMemoSpec extends AnyFunSuite {
  test("fib memo") { assert(Problem019FibMemo.fib(10) == 55L) }
}

