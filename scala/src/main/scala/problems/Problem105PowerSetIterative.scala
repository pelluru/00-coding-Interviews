
package problems
import scala.collection.mutable.ArrayBuffer

object Problem105PowerSetIterative {
  def powerSet(nums:Array[Int]): List[List[Int]] = {
    val res = ArrayBuffer[List[Int]](Nil)
    for (x <- nums) {
      val cur = res.toList
      cur.foreach(s => res += (s :+ x))
    }
    res.toList
  }
}

