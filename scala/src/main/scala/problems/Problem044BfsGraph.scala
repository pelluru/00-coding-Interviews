
package problems
import scala.collection.mutable

object Problem044BfsGraph {
  def bfs(g: Map[Int, List[Int]], s:Int): List[Int] = {
    val seen = mutable.LinkedHashSet[Int](s)
    val q = mutable.Queue[Int](s)
    while (q.nonEmpty) {
      val u = q.dequeue()
      for (v <- g.getOrElse(u, Nil) if !seen.contains(v)) {
        seen += v; q.enqueue(v)
      }
    }
    seen.toList
  }
}

