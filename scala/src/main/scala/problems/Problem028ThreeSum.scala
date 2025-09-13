package problems
import scala.collection.mutable.ArrayBuffer

object Problem028ThreeSum {
  def threeSum(a: Array[Int]): List[List[Int]] = {
    val arr = a.sorted
    val res = ArrayBuffer[List[Int]]()
    for (i <- arr.indices) {
      if (i == 0 || arr(i) != arr(i-1)) {
        var l = i+1; var r = arr.length-1
        while (l < r) {
          val s = arr(i) + arr(l) + arr(r)
          if (s == 0) {
            res += List(arr(i), arr(l), arr(r))
            l += 1; r -= 1
            while (l < r && arr(l) == arr(l-1)) l += 1
            while (l < r && arr(r) == arr(r+1)) r -= 1
          } else if (s < 0) l += 1 else r -= 1
        }
      }
    }
    res.toList
  }
}
