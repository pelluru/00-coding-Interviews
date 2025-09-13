
package problems
import scala.collection.mutable

object Problem090TopologicalSort {
  def topo(n:Int, edges:Array[Array[Int]]): List[Int] = {
    val g = Array.fill(n)(mutable.ListBuffer[Int]())
    val indeg = Array.fill(n)(0)
    for (e <- edges) { g(e(0)) += e(1); indeg(e(1)) += 1 }
    val q = mutable.Queue[Int]()
    for (i <- 0 until n if indeg(i)==0) q.enqueue(i)
    val res = mutable.ListBuffer[Int]()
    while (q.nonEmpty) {
      val u = q.dequeue(); res += u
      for (v <- g(u)) { indeg(v)-=1; if (indeg(v)==0) q.enqueue(v) }
    }
    res.toList
  }
}

