package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem076ThrottleFunction

class Problem076ThrottleFunctionSpec extends AnyFunSuite {
  test("throttle") { val t=new Problem076ThrottleFunction.Throttle(2,1000); assert(t.allow(0) && t.allow(10) && !t.allow(20)) }
}

