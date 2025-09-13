package problems
import scala.collection.mutable.ArrayBuffer

object Problem017Combinations {
  def combine(n:Int, k:Int): List[List[Int]] = {
    val res = ArrayBuffer[List[Int]]()
    def bt(start:Int, cur: List[Int]): Unit = {
      if (cur.length==k) res += cur
      else for (i <- start to n) bt(i+1, cur :+ i)
    }
    bt(1, Nil); res.toList
  }
}
