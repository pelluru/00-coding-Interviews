package problems
object Problem047NQueensCount {
  def count(n:Int): Int = {
    val cols = new Array[Boolean](n)
    val d1 = new Array[Boolean](2*n)
    val d2 = new Array[Boolean](2*n)
    def bt(r:Int): Int = {
      if (r==n) 1
      else (0 until n).filter(c => !cols(c) && !d1(r+c) && !d2(r-c+n)).map { c =>
        cols(c)=true; d1(r+c)=true; d2(r-c+n)=true
        val v=bt(r+1)
        cols(c)=false; d1(r+c)=false; d2(r-c+n)=false
        v
      }.sum
    }
    bt(0)
  }
}
