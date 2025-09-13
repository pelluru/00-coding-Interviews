package problems

object Problem046SieveEratosthenes {
  def sieve(n:Int): List[Int] = {
    val p = Array.fill(n+1)(true)
    if (n>=0) p(0)=false; if (n>=1) p(1)=false
    var i=2; while (i*i<=n) {
      if (p(i)) { var j=i*i; while (j<=n) { p(j)=false; j+=i } }
      i+=1
    }
    (2 to n).filter(p).toList
  }
}
