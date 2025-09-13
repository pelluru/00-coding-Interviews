package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem098DesignCacheTtl

class Problem098DesignCacheTtlSpec extends AnyFunSuite {
  test("ttl cache") { val c=new Problem098DesignCacheTtl.TTLCache[Int,Int](); c.put(1,2,50); assert(c.get(1).contains(2)) }
}

