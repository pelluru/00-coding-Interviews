package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem021LruCacheSimple

class Problem021LruCacheSimpleSpec extends AnyFunSuite {
  test("lru") { val l=new Problem021LruCacheSimple.LRU[Int,Int](2); l.put(1,1); l.put(2,2); l.get(1); l.put(3,3); assert(!l.containsKey(2)) }
}

