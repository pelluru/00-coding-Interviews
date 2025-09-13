package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem029LongestCommonPrefix

class Problem029LongestCommonPrefixSpec extends AnyFunSuite {
  test("lcp") { assert(Problem029LongestCommonPrefix.lcp(Array("flower","flow","flight")) == "fl") }
}

