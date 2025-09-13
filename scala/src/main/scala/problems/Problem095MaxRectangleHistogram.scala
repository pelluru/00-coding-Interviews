
package problems
import scala.collection.mutable

object Problem095MaxRectangleHistogram {
  def largestRectangleArea(h:Array[Int]): Int = {
    val st = new mutable.ArrayDeque[Int]()
    var max = 0; var i=0
    while (i<=h.length) {
      val cur = if (i==h.length) 0 else h(i)
      while (st.nonEmpty && cur < h(st.last)) {
        val height = h(st.removeLast())
        val left = if (st.isEmpty) -1 else st.last
        val width = i - left - 1
        max = math.max(max, height * width)
      }
      st.append(i); i+=1
    }
    max
  }
}

