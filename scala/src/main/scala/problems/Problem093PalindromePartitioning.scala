package problems
object Problem093PalindromePartitioning {
  def partition(s:String): List[List[String]] = {
    val res = scala.collection.mutable.ArrayBuffer[List[String]]()
    def isPal(a:Int,b:Int): Boolean = s.substring(a,b+1) == s.substring(a,b+1).reverse
    def bt(i:Int, cur: List[String]): Unit = {
      if (i==s.length) res += cur
      else {
        var j=i
        while (j<s.length) {
          if (isPal(i,j)) bt(j+1, cur :+ s.substring(i,j+1))
          j+=1
        }
      }
    }
    bt(0, Nil); res.toList
  }
}
