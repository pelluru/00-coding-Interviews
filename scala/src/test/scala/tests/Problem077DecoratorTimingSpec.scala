package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem077DecoratorTiming

class Problem077DecoratorTimingSpec extends AnyFunSuite {
  test("timing") { val (v,ns)=Problem077DecoratorTiming.time{ 1+1 }; assert(v==2 && ns>=0) }
}

