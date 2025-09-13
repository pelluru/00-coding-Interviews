package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem085RetryDecorator

class Problem085RetryDecoratorSpec extends AnyFunSuite {
  test("withRetry") { var x=0; val v=Problem085RetryDecorator.withRetry(3){ x+=1; if(x<2) throw new RuntimeException; 7 }; assert(v==7) }
}

