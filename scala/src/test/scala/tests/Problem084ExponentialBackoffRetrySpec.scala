package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem084ExponentialBackoffRetry

class Problem084ExponentialBackoffRetrySpec extends AnyFunSuite {
  test("retry success") { var x=0; val r=Problem084ExponentialBackoffRetry.retry(3){ x+=1; if(x<2) throw new RuntimeException("no"); 42 }; assert(r==42) }
}

