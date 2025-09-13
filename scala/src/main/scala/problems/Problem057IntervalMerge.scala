
package problems

object Problem057IntervalMerge {
  def merge(intervals:Array[Array[Int]]): Array[Array[Int]] = {
    val arr = intervals.sortBy(_(0))
    val res = scala.collection.mutable.ArrayBuffer[Array[Int]]()
    for (in <- arr) {
      if (res.isEmpty || res.last(1) < in(0)) res += in.clone
      else res.last(1) = math.max(res.last(1), in(1))
    }
    res.toArray
  }
}

