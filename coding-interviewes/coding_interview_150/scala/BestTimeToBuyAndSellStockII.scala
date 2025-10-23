
/* Best Time II — sum positive deltas. O(n) */
object BestTimeToBuyAndSellStockII {
  def maxProfit(prices:Array[Int]):Int = {
    var ans=0
    for (i <- 1 until prices.length) if (prices(i)>prices(i-1)) ans += prices(i)-prices(i-1)
    ans
  }
  def main(args:Array[String]):Unit = {
    println(maxProfit(Array(7,1,5,3,6,4)))
    println(maxProfit(Array(1,2,3,4,5)))
  }
}
