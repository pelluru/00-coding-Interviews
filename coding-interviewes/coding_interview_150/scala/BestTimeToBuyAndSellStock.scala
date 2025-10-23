
/* Best Time I — track min & profit. O(n) */
object BestTimeToBuyAndSellStock {
  def maxProfit(prices:Array[Int]):Int = {
    var mn = Int.MaxValue; var ans = 0
    for (p <- prices){ if (p < mn) mn = p; ans = Math.max(ans, p - mn) }
    ans
  }
  def main(args:Array[String]):Unit = {
    println(maxProfit(Array(7,1,5,3,6,4)))
    println(maxProfit(Array(7,6,4,3,1)))
  }
}
