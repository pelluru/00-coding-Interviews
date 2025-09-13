
package problems
import scala.collection.mutable

object Problem043DijkstraSimple {
  def dijkstra(g: Map[String, List[(String, Int)]], src:String): Map[String, Int] = {
    val dist = mutable.Map[String, Int]().withDefaultValue(Int.MaxValue/4)
    dist(src)=0
    val pq = mutable.PriorityQueue.empty[(Int,String)](Ordering.by[(Int,String),Int](-_._1))
    pq.enqueue((0,src))
    while (pq.nonEmpty) {
      val (d,u) = pq.dequeue()
      if (d <= dist(u)) {
        for ((v,w) <- g.getOrElse(u, Nil)) {
          val nd = d + w
          if (nd < dist(v)) { dist(v)=nd; pq.enqueue((nd,v)) }
        }
      }
    }
    dist.toMap
  }
}

