package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem071ParseCsvLine

class Problem071ParseCsvLineSpec extends AnyFunSuite {
  test("parse csv") { val r=Problem071ParseCsvLine.parse("a,"b,b",c"); assert(r==List("a","b,b","c")) }
}

