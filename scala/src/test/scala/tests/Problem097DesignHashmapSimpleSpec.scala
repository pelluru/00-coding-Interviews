package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem097DesignHashmapSimple

class Problem097DesignHashmapSimpleSpec extends AnyFunSuite {
  test("hashmap") { val m=new Problem097DesignHashmapSimple.MyHashMap[Int,Int](); m.put(1,10); assert(m.get(1).contains(10)); m.remove(1); assert(m.get(1).isEmpty) }
}

