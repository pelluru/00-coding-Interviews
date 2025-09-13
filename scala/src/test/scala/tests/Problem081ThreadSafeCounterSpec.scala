package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem081ThreadSafeCounter

class Problem081ThreadSafeCounterSpec extends AnyFunSuite {
  test("atomic counter") { val c=new Problem081ThreadSafeCounter.Counter; val a=c.inc(); val b=c.inc(); assert(b==a+1) }
}

