package problems
import scala.collection.mutable.ArrayBuffer

object Problem016Permutations {
  def permute(nums:Array[Int]): List[List[Int]] = {
    val res = ArrayBuffer[List[Int]]()
    val used = Array.fill(nums.length)(false)
    def bt(cur: List[Int]): Unit = {
      if (cur.length==nums.length) res += cur
      else {
        for (i <- nums.indices if !used[i]) {
          used[i]=true; bt(cur :+ nums(i)); used[i]=false
        }
      }
    }
    bt(Nil); res.toList
  }
}
