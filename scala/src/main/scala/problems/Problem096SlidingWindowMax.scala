
package problems
import scala.collection.mutable

object Problem096SlidingWindowMax {
  def maxSliding(a:Array[Int], k:Int): Array[Int] = {
    if (a.isEmpty || k==0) return Array()
    val dq = new mutable.ArrayDeque[Int]()
    val res = Array.ofDim[Int](a.length - k + 1)
    var i=0
    while (i<a.length) {
      while (dq.nonEmpty && dq.head <= i-k) dq.removeHead()
      while (dq.nonEmpty && a(dq.last) <= a(i)) dq.removeLast()
      dq.append(i)
      if (i>=k-1) res(i-k+1) = a(dq.head)
      i+=1
    }
    res
  }
}

