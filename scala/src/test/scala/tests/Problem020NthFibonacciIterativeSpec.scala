package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem020NthFibonacciIterative

class Problem020NthFibonacciIterativeSpec extends AnyFunSuite {
  test("fib iterative") { assert(Problem020NthFibonacciIterative.fibN(10) == 55L) }
}

