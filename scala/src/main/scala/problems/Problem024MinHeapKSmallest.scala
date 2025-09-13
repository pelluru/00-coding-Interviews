package problems
object Problem024MinHeapKSmallest {
  def kSmallest(a:Array[Int], k:Int): Array[Int] = a.sorted.take(k)
}
