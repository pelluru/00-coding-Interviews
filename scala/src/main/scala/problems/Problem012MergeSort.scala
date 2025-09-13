package problems

object Problem012MergeSort {
  def sort(a: Array[Int]): Array[Int] = {
    if (a.length <= 1) return a.clone
    val m = a.length / 2
    val left = sort(a.slice(0, m))
    val right = sort(a.slice(m, a.length))
    merge(left, right)
  }
  private def merge(l: Array[Int], r: Array[Int]): Array[Int] = {
    val res = Array.ofDim[Int](l.length + r.length)
    var i=0; var j=0; var k=0
    while (i<l.length && j<r.length) {
      if (l(i) <= r(j)) { res(k)=l(i); i+=1 }
      else { res(k)=r(j); j+=1 }
      k+=1
    }
    while (i<l.length) { res(k)=l(i); i+=1; k+=1 }
    while (j<r.length) { res(k)=r(j); j+=1; k+=1 }
    res
  }
}
