package problems
import scala.collection.mutable

object Problem014KthLargest {
  def kthLargest(a:Array[Int], k:Int): Int = {
    val pq = mutable.PriorityQueue.empty[Int](Ordering.Int.reverse)
    a.foreach { x =>
      pq.enqueue(x)
      if (pq.size > k) pq.dequeue()
    }
    pq.head
  }
}
