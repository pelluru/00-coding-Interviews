package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem073RateLimiterFixedWindow

class Problem073RateLimiterFixedWindowSpec extends AnyFunSuite {
  test("fixed window") { val l=new Problem073RateLimiterFixedWindow.Limiter(2,1000); assert(l.allow(0) && l.allow(1) && !l.allow(2) && l.allow(1000)) }
}

