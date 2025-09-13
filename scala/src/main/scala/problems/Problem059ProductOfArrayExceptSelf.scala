
package problems

object Problem059ProductOfArrayExceptSelf {
  def productExceptSelf(a:Array[Int]): Array[Int] = {
    val n=a.length
    val res = Array.fill(n)(1)
    var pref=1
    var i=0; while (i<n) { res(i)=pref; pref*=a(i); i+=1 }
    var suf=1
    i=n-1; while (i>=0) { res(i)*=suf; suf*=a(i); i-=1 }
    res
  }
}

