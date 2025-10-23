package patterns
object BinarySearch {
  def searchRange(nums:Array[Int], target:Int): Array[Int] = {
    def lower:Int = { var lo=0; var hi=nums.length; while (lo<hi){ val mid=(lo+hi)/2; if (nums(mid)>=target) hi=mid else lo=mid+1 }; lo }
    def upper:Int = { var lo=0; var hi=nums.length; while (lo<hi){ val mid=(lo+hi)/2; if (nums(mid)>target) hi=mid else lo=mid+1 }; lo }
    val l=lower; val r=upper-1
    if (l<nums.length && l<=r && nums(l)==target) Array(l,r) else Array(-1,-1)
  }
  def shipWithinDays(weights:Array[Int], days:Int): Int = {
    var lo=weights.max; var hi=weights.sum
    def ok(cap:Int):Boolean = {
      var d=1; var cur=0
      for (w <- weights){ if (cur+w>cap){ d+=1; cur=0 }; cur+=w }; d<=days
    }
    while (lo<hi){ val mid=(lo+hi)/2; if (ok(mid)) hi=mid else lo=mid+1 }
    lo
  }
  def minEatingSpeed(piles:Array[Int], h:Int): Int = {
    var lo=1; var hi=piles.max
    def ok(v:Int)= piles.map(p => (p+v-1)/v).sum <= h
    while (lo<hi){ val mid=(lo+hi)/2; if (ok(mid)) hi=mid else lo=mid+1 }
    lo
  }
}
