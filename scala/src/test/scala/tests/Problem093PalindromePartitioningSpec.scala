package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem093PalindromePartitioning

class Problem093PalindromePartitioningSpec extends AnyFunSuite {
  test("pal partitions") { val out=Problem093PalindromePartitioning.partition("aab"); assert(out.contains(List("aa","b"))) }
}

