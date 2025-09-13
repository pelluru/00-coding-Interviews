package problems
object Problem019FibMemo {
  private val memo = scala.collection.mutable.HashMap[Int,Long]()
  def fib(n:Int): Long = memo.getOrElseUpdate(n, if (n<2) n else fib(n-1)+fib(n-2))
}
