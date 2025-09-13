package problems

object Problem033CoinChangeMin {
  def coinChange(coins:Array[Int], amount:Int): Int = {
    val INF = 1_000_000
    val dp = Array.fill(amount+1)(INF)
    dp(0)=0
    for (a <- 1 to amount; c <- coins if c<=a) dp[a] = math.min(dp[a], dp[a-c]+1)
    if (dp(amount) >= INF) -1 else dp(amount)
  }
}
