package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem062BucketSortSimple

class Problem062BucketSortSimpleSpec extends AnyFunSuite {
  test("bucket sort [0,1)") { val r=Problem062BucketSortSimple.sort(Array(0.78,0.17,0.39,0.26,0.72,0.94,0.21,0.12,0.23,0.68)); assert(r.head==0.12 && r.last==0.94) }
}

