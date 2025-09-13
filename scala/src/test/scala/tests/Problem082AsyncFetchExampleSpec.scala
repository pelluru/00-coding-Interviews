package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem082AsyncFetchExample

class Problem082AsyncFetchExampleSpec extends AnyFunSuite {
  test("async fetch (lengths)") { import scala.concurrent.Await; val r=Await.result(Problem082AsyncFetchExample.fetchAll(List("a","bbb")), scala.concurrent.duration.Duration(2,"s")); assert(r==List(1,3)) }
}

