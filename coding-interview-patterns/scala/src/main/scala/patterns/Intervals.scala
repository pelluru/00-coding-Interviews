package patterns
import scala.collection.mutable
object Intervals {
  def minMeetingRooms(intervals:Array[Array[Int]]): Int = {
    val arr = intervals.sortBy(_(0))
    val pq = mutable.PriorityQueue[Int]()(Ordering.Int.reverse) // min-heap
    for (iv <- arr){
      if (pq.nonEmpty && pq.head <= iv(0)) { pq.dequeue(); pq.enqueue(iv(1)) }
      else pq.enqueue(iv(1))
    }
    pq.size
  }
  def eraseOverlapIntervals(intervals:Array[Array[Int]]): Int = {
    if (intervals.isEmpty) return 0
    val arr = intervals.sortBy(_(1))
    var end=arr(0)(1); var keep=1
    for (i<-1 until arr.length){ val s=arr(i)(0); val e=arr(i)(1); if (s>=end){ keep+=1; end=e } }
    arr.length - keep
  }
  def employeeFreeTime(schedules:Array[Array[Array[Int]]]): Array[Array[Int]] = {
    val intervals = schedules.flatMap(identity).sortBy(_(0))
    val merged = mutable.ArrayBuffer[Array[Int]]()
    for (iv <- intervals){
      if (merged.isEmpty || merged.last(1) < iv(0)) merged += Array(iv(0), iv(1))
      else merged.last(1) = merged.last(1).max(iv(1))
    }
    val free = mutable.ArrayBuffer[Array[Int]]()
    for (i<-1 until merged.length) if (merged(i-1)(1) < merged(i)(0)) free += Array(merged(i-1)(1), merged(i)(0))
    free.toArray
  }
}
