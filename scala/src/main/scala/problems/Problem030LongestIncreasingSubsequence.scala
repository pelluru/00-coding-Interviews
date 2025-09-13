package problems

object Problem030LongestIncreasingSubsequence {
  def lis(a: Array[Int]): Int = {
    val tails = Array.fill(a.length)(0)
    var size = 0
    for (x <- a) {
      var i = java.util.Arrays.binarySearch(tails, 0, size, x)
      if (i < 0) i = -(i+1)
      tails(i) = x
      if (i == size) size += 1
    }
    size
  }
}
