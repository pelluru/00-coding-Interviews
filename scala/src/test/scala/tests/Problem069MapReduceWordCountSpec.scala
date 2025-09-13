package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem069MapReduceWordCount

class Problem069MapReduceWordCountSpec extends AnyFunSuite {
  test("word count") { val r=Problem069MapReduceWordCount.wordCount(List("A a", "a b")); assert(r("a")==3 && r("b")==1) }
}

