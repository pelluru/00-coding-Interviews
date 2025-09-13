package problems
import scala.collection.mutable

object Problem107KClosestPointsToOrigin {
  def kClosest(points:Array[Array[Int]], k:Int): Array[Array[Int]] = {
    val pq = mutable.PriorityQueue[Array[Int]]()(Ordering.by(p => p(0)*p(0) + p(1)*p(1)))
    for (p <- points) {
      pq.enqueue(p)
      if (pq.size > k) pq.dequeue()
    }
    pq.toArray
  }
}

