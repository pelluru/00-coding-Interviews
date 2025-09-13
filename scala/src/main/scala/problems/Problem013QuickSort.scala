package problems

object Problem013QuickSort {
  def sort(a: Array[Int]): Unit = qs(a, 0, a.length-1)
  private def qs(a: Array[Int], l: Int, r: Int): Unit = {
    if (l >= r) return
    val p = a((l+r)/2)
    var i = l; var j = r
    while (i <= j) {
      while (a(i) < p) i += 1
      while (a(j) > p) j -= 1
      if (i <= j) { val t=a(i); a(i)=a(j); a(j)=t; i+=1; j-=1 }
    }
    qs(a, l, j); qs(a, i, r)
  }
}
