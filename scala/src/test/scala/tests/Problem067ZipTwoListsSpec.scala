package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem067ZipTwoLists

class Problem067ZipTwoListsSpec extends AnyFunSuite {
  test("zip") { assert(Problem067ZipTwoLists.zip2(List(1,2), List("a","b")) == List((1,"a"),(2,"b"))) }
}

