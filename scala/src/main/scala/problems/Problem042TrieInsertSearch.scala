
package problems
import scala.collection.mutable

object Problem042TrieInsertSearch {
  final class TrieNode { val c = mutable.HashMap.empty[Char, TrieNode]; var end=false }
  final class Trie {
    private val root = new TrieNode
    def insert(w:String): Unit = {
      var n = root
      w.foreach { ch => n = n.c.getOrElseUpdate(ch, new TrieNode) }
      n.end = true
    }
    def search(w:String): Boolean = {
      var n = root
      for (ch <- w) {
        if (!n.c.contains(ch)) return false
        n = n.c(ch)
      }
      n.end
    }
  }
}

