
package problems

object Problem061HeapSort {
  def sort(a:Array[Int]): Unit = {
    val n=a.length
    var i=n/2-1; while (i>=0) { heapify(a, n, i); i-=1 }
    i=n-1; while (i>=0) {
      val t=a(0); a(0)=a(i); a(i)=t
      heapify(a, i, 0); i-=1
    }
  }
  private def heapify(a:Array[Int], n:Int, i:Int): Unit = {
    var largest=i; val l=2*i+1; val r=2*i+2
    if (l<n && a(l)>a(largest)) largest=l
    if (r<n && a(r)>a(largest)) largest=r
    if (largest!=i) { val t=a(i); a(i)=a(largest); a(largest)=t; heapify(a,n,largest) }
  }
}

