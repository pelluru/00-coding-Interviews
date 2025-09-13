package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem094MinWindowSubString

class Problem094MinWindowSubStringSpec extends AnyFunSuite {
  test("alias min window") { assert(Problem094MinWindowSubString.minWindow("ADOBECODEBANC","ABC")=="BANC") }
}

