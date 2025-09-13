
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem042TrieInsertSearch

class Problem042TrieInsertSearchSpec extends AnyFunSuite {
  test("trie") {
    val t = new Problem042TrieInsertSearch.Trie
    t.insert("hi")
    assert(t.search("hi") && !t.search("h"))
  }
}

