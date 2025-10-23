package patterns
object DP1D {
  def coinChangeMin(coins:Array[Int], amount:Int): Int = {
    val INF=1000000000; val dp=Array.fill(amount+1)(INF); dp(0)=0
    for (s<-1 to amount) for (c<-coins) if (s-c>=0) dp(s)=dp(s).min(dp(s-c)+1)
    if (dp(amount)>=INF) -1 else dp(amount)
  }
  def lisLength(nums:Array[Int]): Int = {
    import scala.collection.mutable.ArrayBuffer
    val tails = ArrayBuffer[Int]()
    for (x <- nums){
      var lo=0; var hi=tails.length
      while (lo<hi){
        val mid=(lo+hi)/2
        if (tails(mid) >= x) hi=mid else lo=mid+1
      }
      if (lo==tails.length) tails += x else tails(lo)=x
    }
    tails.length
  }
  def houseRobber(nums:Array[Int]): Int = {
    var take=0; var skip=0
    for (x <- nums){ val ntake=skip+x; val nskip=take.max(skip); take=ntake; skip=nskip }
    take.max(skip)
  }
}
