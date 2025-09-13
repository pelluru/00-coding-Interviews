
package problems

object Problem058BestTimeToBuySellStock {
  def maxProfit(prices:Array[Int]): Int = {
    var minP = prices(0); var best = 0
    for (p <- prices) { if (p<minP) minP=p; best = math.max(best, p-minP) }
    best
  }
}

