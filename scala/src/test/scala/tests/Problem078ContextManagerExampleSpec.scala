package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem078ContextManagerExample

class Problem078ContextManagerExampleSpec extends AnyFunSuite {
  test("using") { class R extends AutoCloseable { var closed=false; def close()= { closed=true } }; val r=new R; Problem078ContextManagerExample.using(r)(_ => ()); assert(r.closed) }
}

