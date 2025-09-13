
package problems

object Problem088UnionFind {
  final class UF(n:Int){
    private val p = Array.tabulate(n)(identity)
    private val r = Array.fill(n)(0)
    def find(x:Int): Int = { if (p(x)!=x) p(x)=find(p(x)); p(x) }
    def union(a:Int,b:Int): Unit = {
      val ra=find(a); val rb=find(b)
      if (ra==rb) return
      if (r[ra] < r[rb]) p(ra)=rb
      else if (r[ra] > r[rb]) p(rb)=ra
      else { p(rb)=ra; r(ra)+=1 }
    }
  }
}

