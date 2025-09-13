package problems

object Problem031EditDistance {
  def edit(a: String, b: String): Int = {
    val m=a.length; val n=b.length
    val dp = Array.ofDim[Int](m+1, n+1)
    for (i <- 0 to m) dp(i)(0) = i
    for (j <- 0 to n) dp(0)(j) = j
    for (i <- 1 to m; j <- 1 to n) {
      dp(i)(j) =
        if (a(i-1)==b(j-1)) dp(i-1)(j-1)
        else 1 + math.min(dp(i-1)(j-1), math.min(dp(i-1)(j), dp(i)(j-1)))
    }
    dp(m)(n)
  }
}
