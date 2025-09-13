package problems

object Problem015KadaneMaxSubarray {
  def maxSubarray(a:Array[Int]): Int = {
    var best=a(0); var cur=a(0)
    for (i <- 1 until a.length) {
      cur = math.max(a(i), cur + a(i))
      best = math.max(best, cur)
    }
    best
  }
}
