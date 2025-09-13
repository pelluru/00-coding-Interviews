package problems
object Problem020NthFibonacciIterative {
  def fibN(n:Int): Long = { if (n<2) n else (2 to n).foldLeft((0L,1L)){case ((a,b),_) => (b,a+b)}._2 }
}
