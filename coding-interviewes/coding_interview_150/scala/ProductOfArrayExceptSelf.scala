
/* Product Except Self — prefix/suffix. O(n) */
object ProductOfArrayExceptSelf {
  def productExceptSelf(nums:Array[Int]):Array[Int] = {
    val n=nums.length
    val out=Array.fill(n)(1)
    var pref=1; var i=0
    while (i<n){ out(i)=pref; pref*=nums(i); i+=1 }
    var suff=1; i=n-1
    while (i>=0){ out(i)*=suff; suff*=nums(i); i-=1 }
    out
  }
  def main(args:Array[String]):Unit = {
    println(productExceptSelf(Array(1,2,3,4)).mkString("[",",","]"))
  }
}
