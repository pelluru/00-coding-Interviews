package problems

object Problem035LongestPalindromicSubstring {
  def lps(s: String): String = {
    if (s==null || s.isEmpty) return ""
    var best=(0,0)
    def expand(L:Int,R:Int): (Int,Int) = {
      var l=L; var r=R
      while (l>=0 && r<s.length && s(l)==s(r)) { l-=1; r+=1 }
      (l+1,r-1)
    }
    for (i <- s.indices) {
      val a = expand(i,i); val b = expand(i,i+1)
      val pick = if (a._2-a._1 >= b._2-b._1) a else b
      if (pick._2-pick._1 > best._2-best._1) best = pick
    }
    s.substring(best._1, best._2+1)
  }
}
