package patterns
object Greedy {
  def mergeIntervals(intervals:Array[Array[Int]]): Array[Array[Int]] = {
    val arr = intervals.sortBy(_(0))
    val out = scala.collection.mutable.ArrayBuffer[Array[Int]]()
    for (iv <- arr){
      if (out.isEmpty || out.last(1) < iv(0)) out += Array(iv(0), iv(1))
      else out.last(1) = out.last(1).max(iv(1))
    }
    out.toArray
  }
  def eraseOverlapIntervals(intervals:Array[Array[Int]]): Int = {
    if (intervals.isEmpty) return 0
    val arr = intervals.sortBy(_(1))
    var end=arr(0)(1); var keep=1
    for (i<-1 until arr.length){ val s=arr(i)(0); val e=arr(i)(1); if (s>=end){ keep+=1; end=e } }
    arr.length - keep
  }
  def minArrows(points:Array[Array[Int]]): Int = {
    if (points.isEmpty) return 0
    val arr = points.sortBy(_(1))
    var end=arr(0)(1); var arrows=1
    for (i<-1 until arr.length){ val s=arr(i)(0); val e=arr(i)(1); if (s>end){ arrows+=1; end=e } }
    arrows
  }
}
