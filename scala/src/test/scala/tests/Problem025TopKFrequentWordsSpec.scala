package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem025TopKFrequentWords

class Problem025TopKFrequentWordsSpec extends AnyFunSuite {
  test("top k words") { assert(Problem025TopKFrequentWords.topK(Array("a","b","a"),1) == List("a")) }
}

