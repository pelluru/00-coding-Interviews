package patterns
import scala.collection.mutable
object PrefixSums {
  def subarraySumK(nums:Array[Int], k:Int): Int = {
    val map = mutable.Map[Int,Int](0->1)
    var pref=0; var ans=0
    for (x <- nums){ pref+=x; ans += map.getOrElse(pref-k,0); map.update(pref, map.getOrElse(pref,0)+1) }
    ans
  }
  class NumMatrix(M:Array[Array[Int]]){
    private val m=M.length; private val n=if (m==0) 0 else M(0).length
    private val P = Array.ofDim[Int](m+1, n+1)
    for (i<-1 to m; j<-1 to n) P(i)(j)=M(i-1)(j-1)+P(i-1)(j)+P(i)(j-1)-P(i-1)(j-1)
    def sumRegion(r1:Int,c1:Int,r2:Int,c2:Int): Int =
      P(r2+1)(c2+1)-P(r1)(c2+1)-P(r2+1)(c1)+P(r1)(c1)
  }
  def applyRangeAdds(n:Int, updates:Array[Array[Int]]): Array[Int] = {
    val diff = Array.fill(n+1)(0)
    for (u <- updates){ val l=u(0); val r=u(1); val v=u(2); diff(l)+=v; if (r+1<diff.length) diff(r+1)-=v }
    val out = Array.fill(n)(0); var cur=0
    for (i<-0 until n){ cur+=diff(i); out(i)=cur }
    out
  }
}
