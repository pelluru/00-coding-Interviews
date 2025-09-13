package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem074TokenBucketLimiter

class Problem074TokenBucketLimiterSpec extends AnyFunSuite {
  test("token bucket") { val b=new Problem074TokenBucketLimiter.TokenBucket(1.0,1); assert(b.allow(0)); assert(!b.allow(10)); assert(b.allow(1100)) }
}

