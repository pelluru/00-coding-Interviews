package problems


object Problem060MaxProfitKTransactions {
  def maxProfit(k:Int, prices:Array[Int]): Int = {
    val n=prices.length; if (n==0 || k==0) return 0
    if (k >= n/2) {
      var prof=0; var i=1; while (i<n) { if (prices(i)>prices(i-1)) prof+=prices(i)-prices(i-1); i+=1 }; return prof
    }
    val buy = Array.fill(k+1)(Int.MinValue/4)
    val sell = Array.fill(k+1)(0)
    for (p <- prices) {
      var t=1; while (t<=k) {
        buy(t) = math.max(buy(t), sell(t-1) - p)
        sell(t) = math.max(sell(t), buy(t) + p)
        t+=1
      }
    }
    sell(k)
  }
}

